#![allow(dead_code)]

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;

use chrono::{Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_repr::{Deserialize_repr, Serialize_repr};

pub async fn setup_sync(
    pg_addr: SocketAddr,
    gel_addr: SocketAddr,
) -> anyhow::Result<CreatedObjects> {
    let client = Client::new();

    let group = create_group(&client).await?;
    let destination = create_destination(&client, &group.id, pg_addr).await?;
    log::debug!("destination = {destination:#?}");

    let mut connector = create_connector(&client, &group.id, gel_addr).await?;
    log::debug!("connector = {connector:#?}");
    while connector.status.setup_state != "connected" {
        log::info!("waiting for connector to have `setup_state` == \"connected\"");
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        connector = get_connector(&client, &connector.id).await?;
        log::debug!("connector.status = {:#?}", connector.status);
    }

    let schema = reload_connector_schema_config(&client, &connector.id).await?;
    log::trace!("schema = {schema:#?}");

    update_connector_schema_config(
        &client,
        &connector.id,
        &UpdateConnectorSchemaRequest {
            schema_change_handling: SchemaChangeHandling::BlockAll,
            schemas: pick_schema(schema),
        },
    )
    .await?;

    let mut connector = start_sync(&client, &connector.id).await?;
    log::debug!("connector.status = {:#?}", connector.status);
    while connector.failed_at.is_none() && connector.succeeded_at.is_none() {
        log::info!("waiting for connector sync to succeed or fail");
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        connector = get_connector(&client, &connector.id).await?;
        log::debug!("connector.status = {:#?}", connector.status);
    }

    log::debug!("connector = {:#?}", connector);

    if connector.failed_at.is_some() {
        log::error!("failed")
    } else {
        log::info!("succeeded");
    }
    Ok(CreatedObjects { group, destination })
}

/// Picks schema objects that we want to sync.
fn pick_schema(schema: StandardConfigResponse) -> HashMap<String, UpdateConnectorSchema> {
    const SKIP_COLUMNS: &[(&str, &str, &str)] = &[
        // Don't sync username, because it is a computed that needs a global,
        // and we don't support globals over COPY yet.
        ("public", "Person", "username"),
    ];

    schema
        .schemas
        .into_iter()
        .map(|(s_name, s)| {
            let s_name_ref = s_name.as_str();
            let s = UpdateConnectorSchema {
                enabled: true,
                tables: s
                    .tables
                    .into_iter()
                    .map(|(t_name, t)| {
                        let t_name_ref = t_name.as_str();
                        let t = UpdateConnectorTable {
                            enabled: true,
                            columns: t
                                .columns
                                .into_iter()
                                .map(|(c_name, c)| {
                                    let enabled = c.enabled
                                        && !SKIP_COLUMNS
                                            .contains(&(s_name_ref, t_name_ref, &c_name));
                                    let c = UpdateConnectorColumn {
                                        enabled,
                                        hashed: Some(false),
                                        is_primary_key: c.is_primary_key,
                                    };
                                    (c_name, c)
                                })
                                .collect(),
                        };
                        (t_name, t)
                    })
                    .collect(),
            };
            (s_name, s)
        })
        .collect()
}

pub struct CreatedObjects {
    group: GroupResponse,
    destination: DestinationExtendedResponse,
}

pub async fn cleanup(objects: &CreatedObjects) -> anyhow::Result<()> {
    log::info!("cleaning up");

    let client = Client::new();

    let connectors = list_connectors_of_group(&client, &objects.group.id).await?;
    for connector in &connectors.items {
        delete_connector(&client, &connector.id).await?;
    }
    delete_destination(&client, &objects.destination.id).await?;
    delete_group(&client, &objects.group.id).await?;

    Ok(())
}

pub async fn cleanup_old() -> anyhow::Result<()> {
    let client = Client::new();

    log::info!("removing old connectors");
    let connectors = list_connectors(&client).await?;
    for connector in &connectors.items {
        if is_old(&connector.created_at) {
            delete_connector(&client, &connector.id).await?;
        }
    }

    log::info!("removing old groups & destinations");
    let destinations = list_destinations(&client).await?;
    for destination in destinations.items {
        let group = get_group(&client, &destination.group_id).await?;

        if is_old(&group.created_at) {
            delete_destination(&client, &destination.id).await?;
            delete_group(&client, &destination.group_id).await?;
        }
    }

    Ok(())
}

fn is_old(created_at: &str) -> bool {
    let Ok(created_at) = chrono::DateTime::parse_from_str(created_at, "%+") else {
        return true;
    };
    let since_created = Utc::now().signed_duration_since(created_at);

    since_created.num_minutes() > 15
}

struct Client {
    base_url: reqwest::Url,
    inner: reqwest::Client,
}

impl Client {
    fn new() -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.append(
            "Authorization",
            reqwest::header::HeaderValue::from_str(&env::var("FIVETRAN_AUTHORIZATION").unwrap())
                .unwrap(),
        );

        let inner = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .build()
            .unwrap();
        Client {
            inner,
            base_url: reqwest::Url::parse("https://api.fivetran.com").unwrap(),
        }
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.inner
            .request(method, self.base_url.join(path).unwrap())
    }
}

async fn receive_api_response<R: DeserializeOwned + std::fmt::Debug>(
    response: reqwest::Response,
) -> anyhow::Result<R> {
    if let Some(r) = receive_api_response_maybe(response).await? {
        Ok(r)
    } else {
        Err(anyhow::anyhow!("request failed: no data"))
    }
}

async fn receive_api_response_empty(response: reqwest::Response) -> anyhow::Result<()> {
    receive_api_response_maybe::<()>(response).await?;
    Ok(())
}

async fn receive_api_response_maybe<R: DeserializeOwned + std::fmt::Debug>(
    response: reqwest::Response,
) -> anyhow::Result<Option<R>> {
    let status = response.status();
    let r = response.json::<ApiResponse<R>>().await;

    match r {
        Err(err) => {
            log::error!("  {} {:?}", status, err);
            Err(anyhow::anyhow!("request failed"))
        }

        Ok(r) => {
            log::info!("  {} {:?}", status, r.message);
            Ok(r.data)
        }
    }
}

#[derive(Deserialize, Debug)]
struct ApiResponse<R> {
    code: String,
    data: Option<R>,
    message: Option<String>,
}

// --- group ---

async fn create_group(client: &Client) -> anyhow::Result<GroupResponse> {
    let now = chrono::Utc::now();
    let group_name = format!(
        "test_{:04}_{:02}_{:02}T{:02}_{:02}_{:02}",
        now.year(),
        now.month(),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    );

    log::info!("create_group: {group_name}");
    let res = client
        .request(reqwest::Method::POST, "/v1/groups")
        .json(&NewGroupRequest {
            name: group_name.clone(),
        })
        .send()
        .await?;

    receive_api_response(res).await
}

#[derive(Serialize)]
struct NewGroupRequest {
    name: String,
}

#[derive(Deserialize, Debug)]
struct GroupResponse {
    id: String,
    name: String,
    created_at: String,
}

async fn delete_group(client: &Client, group_id: &str) -> anyhow::Result<()> {
    log::info!("delete_group: {group_id}");

    let res = client
        .request(reqwest::Method::DELETE, &format!("/v1/groups/{group_id}"))
        .send()
        .await?;

    receive_api_response_empty(res).await
}

async fn get_group(client: &Client, group_id: &str) -> anyhow::Result<GroupResponse> {
    log::info!("get_group: {group_id}");

    let res = client
        .request(reqwest::Method::GET, &format!("/v1/groups/{group_id}"))
        .send()
        .await?;

    receive_api_response(res).await
}

// --- destination ---

async fn create_destination(
    client: &Client,
    group_id: &str,
    pg_addr: SocketAddr,
) -> anyhow::Result<DestinationExtendedResponse> {
    log::info!("create_destination");

    let res = client
        .request(reqwest::Method::POST, "/v1/destinations")
        .json(&PostgresWarehouseNewDestinationRequest {
            group_id: group_id.to_string(),
            service: "postgres_warehouse".into(),
            time_zone_offset: TimeZoneOffset::utc,
            region: None,
            trust_certificates: Some(true),
            trust_fingerprints: Some(true),
            run_setup_tests: Some(true),
            daylight_saving_time_enabled: None,
            hybrid_deployment_agent_id: None,
            private_link_id: None,
            proxy_agent_id: None,
            config: PostgresWarehouseConfigV1Config {
                host: Some(pg_addr.ip().to_string()),
                port: Some(pg_addr.port() as i64),
                user: Some("username".into()),
                password: Some("pass".into()),
                database: Some("postgres".into()),
                always_encrypted: Some(false),
                connection_type: Some(ConnectionType::Directly),
                ..Default::default()
            },
        })
        .send()
        .await?;

    receive_api_response(res).await
}

#[derive(Serialize)]
struct PostgresWarehouseNewDestinationRequest {
    group_id: String,
    service: String,
    time_zone_offset: TimeZoneOffset,
    region: Option<Region>,
    trust_certificates: Option<bool>,
    trust_fingerprints: Option<bool>,
    run_setup_tests: Option<bool>,
    daylight_saving_time_enabled: Option<bool>,
    hybrid_deployment_agent_id: Option<String>,
    private_link_id: Option<String>,
    proxy_agent_id: Option<String>,
    config: PostgresWarehouseConfigV1Config,
}

#[derive(Serialize, Default)]
struct PostgresWarehouseConfigV1Config {
    tunnel_port: Option<i64>,
    database: Option<String>,
    password: Option<String>,
    connection_type: Option<ConnectionType>,
    port: Option<i64>,
    host: Option<String>,
    tunnel_host: Option<String>,
    always_encrypted: Option<bool>,
    user: Option<String>,
    tunnel_user: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
enum ConnectionType {
    Directly,
    PrivateLink,
    ProxyAgent,
    SshTunnel,
}

#[derive(Serialize, Deserialize, Debug)]
#[allow(non_camel_case_types)]
enum Region {
    AWS_AP_NORTHEAST_1,
    AWS_AP_SOUTHEAST_1,
    AWS_AP_SOUTHEAST_2,
    AWS_AP_SOUTH_1,
    AWS_CA_CENTRAL_1,
    AWS_EU_CENTRAL_1,
    AWS_EU_WEST_1,
    AWS_EU_WEST_2,
    AWS_US_EAST_1,
    AWS_US_EAST_2,
    AWS_US_GOV_WEST_1,
    AWS_US_WEST_2,
    AZURE_AUSTRALIAEAST,
    AZURE_CANADACENTRAL,
    AZURE_CENTRALINDIA,
    AZURE_CENTRALUS,
    AZURE_EASTUS,
    AZURE_EASTUS2,
    AZURE_JAPANEAST,
    AZURE_SOUTHEASTASIA,
    AZURE_UAENORTH,
    AZURE_UKSOUTH,
    AZURE_WESTEUROPE,
    GCP_ASIA_NORTHEAST1,
    GCP_ASIA_SOUTH1,
    GCP_ASIA_SOUTHEAST1,
    GCP_ASIA_SOUTHEAST2,
    GCP_AUSTRALIA_SOUTHEAST1,
    GCP_EUROPE_WEST2,
    GCP_EUROPE_WEST3,
    GCP_NORTHAMERICA_NORTHEAST1,
    GCP_US_CENTRAL1,
    GCP_US_EAST4,
    GCP_US_WEST1,
}

#[derive(Serialize, Deserialize, Debug)]
#[allow(non_camel_case_types)]
enum TimeZoneOffset {
    #[serde(rename = "-11")]
    minus_11,
    #[serde(rename = "-10")]
    minus_10,
    #[serde(rename = "-9")]
    minus_9,
    #[serde(rename = "-8")]
    minus_8,
    #[serde(rename = "-7")]
    minus_7,
    #[serde(rename = "-6")]
    minus_6,
    #[serde(rename = "-5")]
    minus_5,
    #[serde(rename = "-4")]
    minus_4,
    #[serde(rename = "-3")]
    minus_3,
    #[serde(rename = "-2")]
    minus_2,
    #[serde(rename = "-1")]
    minus_1,
    #[serde(rename = "0")]
    utc,
    #[serde(rename = "+1")]
    plus_1,
    #[serde(rename = "+2")]
    plus_2,
    #[serde(rename = "+3")]
    plus_3,
    #[serde(rename = "+4")]
    plus_4,
    #[serde(rename = "+5")]
    plus_5,
    #[serde(rename = "+6")]
    plus_6,
    #[serde(rename = "+7")]
    plus_7,
    #[serde(rename = "+8")]
    plus_8,
    #[serde(rename = "+9")]
    plus_9,
    #[serde(rename = "+10")]
    plus_10,
    #[serde(rename = "+11")]
    plus_11,
    #[serde(rename = "+12")]
    plus_12,
}

#[derive(Deserialize, Debug)]
#[allow(non_camel_case_types)]
enum DestinationSetupStatus {
    broken,
    connected,
    incomplete,
}

#[derive(Deserialize, Debug)]
struct DestinationExtendedResponse {
    id: String,
    service: String,
    region: Region,
    setup_status: DestinationSetupStatus,
    group_id: String,
    time_zone_offset: TimeZoneOffset,
    daylight_saving_time_enabled: Option<bool>,
    // setup_tests: Option<list["SetupTestResultResponse"]>,
    local_processing_agent_id: Option<String>,
    private_link_id: Option<String>,
    proxy_agent_id: Option<String>,
    hybrid_deployment_agent_id: Option<String>,
}

async fn delete_destination(client: &Client, destination_id: &str) -> anyhow::Result<()> {
    log::info!("delete_destination: {destination_id}");

    let res = client
        .request(
            reqwest::Method::DELETE,
            &format!("/v1/destinations/{destination_id}"),
        )
        .send()
        .await?;

    receive_api_response_empty(res).await
}

async fn list_destinations(client: &Client) -> anyhow::Result<ListDestinationResponse> {
    log::info!("list_destinations");

    let res = client
        .request(reqwest::Method::GET, "/v1/destinations")
        .send()
        .await?;

    receive_api_response(res).await
}

#[derive(Deserialize, Debug)]
struct ListDestinationResponse {
    items: Vec<DestinationResponse>,
}

#[derive(Deserialize, Debug)]
struct DestinationResponse {
    group_id: String,
    id: String,
    region: Region,
    service: String,
    setup_status: DestinationSetupStatus,
    time_zone_offset: TimeZoneOffset,
}

// --- connection ---

async fn create_connector(
    client: &Client,
    group_id: &str,
    gel_addr: SocketAddr,
) -> anyhow::Result<ConnectorResponseV1> {
    log::info!("create_connection");

    let config = PostgresConfigV1Config {
        host: Some(gel_addr.ip().to_string()),
        port: Some(gel_addr.port()),
        user: Some("edgedb".into()),
        password: Some("edgedb".into()),
        database: Some("main".into()),
        update_method: Some(PostgresConfigV1ConfigUpdateMethod::XMIN),
        connection_type: Some(ConnectionType::Directly),
        schema_prefix: "gel".into(),
        ..Default::default()
    };

    let res = client
        .request(reqwest::Method::POST, "/v1/connections")
        .json(&PostgresNewConnectorRequestV1 {
            group_id: Some(group_id.to_string()),
            service: Some("postgres".into()),
            trust_certificates: Some(true),
            trust_fingerprints: Some(true),
            run_setup_tests: Some(true),
            paused: Some(true),
            pause_after_trial: Some(true),
            sync_frequency: Some(NewConnectorRequestV1SyncFrequency::Value15),
            daily_sync_time: None,
            config,
        })
        .send()
        .await?;

    receive_api_response(res).await
}

#[derive(Serialize, Default)]
struct PostgresConfigV1Config {
    publication_name: Option<String>,
    connection_type: Option<ConnectionType>,
    update_method: Option<PostgresConfigV1ConfigUpdateMethod>,
    always_encrypted: Option<bool>,
    tunnel_user: Option<String>,
    client_public_certificate: Option<String>,
    tunnel_port: Option<u16>,
    // auth_method: Option<PostgresConfigV1ConfigAuthMethod>,
    database: Option<String>,
    password: Option<String>,
    client_private_key: Option<String>,
    port: Option<u16>,
    host: Option<String>,
    tunnel_host: Option<String>,
    entra_tenant_id: Option<String>,
    entra_app_id: Option<String>,
    replication_slot: Option<String>,
    user: Option<String>,

    schema_prefix: String,
}

#[derive(Serialize)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
enum PostgresConfigV1ConfigUpdateMethod {
    TELEPORT,
    WAL,
    WAL_PGOUTPUT,
    XMIN,
}

#[derive(Serialize)]
struct PostgresNewConnectorRequestV1 {
    group_id: Option<String>,
    service: Option<String>,
    trust_certificates: Option<bool>,
    trust_fingerprints: Option<bool>,
    run_setup_tests: Option<bool>,
    paused: Option<bool>,
    pause_after_trial: Option<bool>,
    sync_frequency: Option<NewConnectorRequestV1SyncFrequency>,
    // data_delay_sensitivity: Option<NewConnectorRequestV1DataDelaySensitivity>,
    // data_delay_threshold: Option<int>,
    daily_sync_time: Option<String>,
    // schedule_type: Option<NewConnectorRequestV1ScheduleType>,
    // connect_card_config: Option<"ConnectCardConfig">,
    // proxy_agent_id: Option<String>,
    // private_link_id: Option<String>,
    // networking_method: Option<NewConnectorRequestV1NetworkingMethod>,
    // hybrid_deployment_agent_id: Option<String>,
    config: PostgresConfigV1Config,
}

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug)]
#[repr(u16)]
enum NewConnectorRequestV1SyncFrequency {
    Value1 = 1,
    Value5 = 5,
    Value15 = 15,
    Value30 = 30,
    Value60 = 60,
    Value120 = 120,
    Value180 = 180,
    Value360 = 360,
    Value480 = 480,
    Value720 = 720,
    Value1440 = 1440,
}

#[derive(Deserialize, Debug)]
struct ConnectorResponseV1 {
    id: String,
    service: String,
    schema: String,
    paused: bool,
    status: ConnectorStatusResponse,
    sync_frequency: NewConnectorRequestV1SyncFrequency,
    group_id: String,
    service_version: i64,
    created_at: String,
    pause_after_trial: bool,
    // data_delay_sensitivity: ConnectorResponseV1DataDelaySensitivity,
    schedule_type: String,
    daily_sync_time: Option<String>,
    succeeded_at: Option<String>,
    connected_by: Option<String>,
    // setup_tests: Option<list["SetupTestResultResponse"]>,
    // source_sync_details: Option<"ConnectorResponseV1SourceSyncDetails">,
    failed_at: Option<String>,
    private_link_id: Option<String>,
    proxy_agent_id: Option<String>,
    // networking_method: Option<ConnectorResponseV1NetworkingMethod>,
    // connect_card: Option<"ConnectCardResponse">,
    // data_delay_threshold: Option<int64>,
    // connect_card_config: Option<"ConnectCardConfig">,
    hybrid_deployment_agent_id: Option<String>,
}

#[derive(Deserialize, Debug)]
struct ConnectorStatusResponse {
    ///    update_state (str): The current data update state of the connection. The available values are: <br /> -
    ///        on_schedule - the sync is running smoothly, no delays <br /> - delayed - the data is delayed for a longer time
    ///        than expected for the update. Example: delayed.
    update_state: String,

    /// setup_state (str): The current setup state of the connection. The available values are: <br /> - incomplete -
    ///         the setup config is incomplete, the setup tests never succeeded <br /> - connected - the connection is properly
    ///         set up <br /> - broken - the connection setup config is broken. Example: connected.
    setup_state: String,

    /// sync_state (str): The current sync state of the connection. The available values are: <br /> - scheduled - the
    ///         sync is waiting to be run <br /> - syncing - the sync is currently running <br /> - paused - the sync is
    ///         currently paused <br /> - rescheduled - the sync is waiting until more API calls are available in the source
    ///         service. Example: scheduled.
    sync_state: String,

    /// is_historical_sync (bool): The boolean specifying whether the connection should be triggered to re-sync all
    ///         historical data. If you set this parameter to TRUE, the next scheduled sync will be historical. If the value is
    ///         FALSE or not specified, the connection will not re-sync historical data. NOTE: When the value is TRUE, only the
    ///         next scheduled sync will be historical, all subsequent ones will be incremental. This parameter is set to FALSE
    ///         once the historical sync is completed.
    is_historical_sync: bool,

    // /// tasks (Union[Unset, list['Alert']]): The collection of tasks for the connection
    // tasks: Union[Unset, list["Alert"]] = UNSET

    // /// warnings (Union[Unset, list['Alert']]): The collection of warnings for the connection
    // warnings: Union[Unset, list["Alert"]] = UNSET
    /// schema_status (Union[Unset, str]): Schema status. Returned only for connectors that support [Universal Column
    /// Masking flow](https://fivetran.com/docs/rest-api/tutorials/schema-status). Example: ready.
    schema_status: Option<String>,

    /// rescheduled_for (Union[Unset, datetime.datetime]): The scheduled time for the next sync when sync_state is
    /// rescheduled. If schedule_type is manual, then the connection expects triggering the event at the designated time
    /// through the [Sync Connection Data](https://fivetran.com/docs/rest-api/api-reference/connections/sync-connection)
    /// endpoint. Example: 2024-12-01T15:43:29.013729Z.
    rescheduled_for: Option<String>,
}

async fn start_sync(client: &Client, connection_id: &str) -> anyhow::Result<ConnectorResponseV1> {
    log::info!("start_sync");

    let res = client
        .request(
            reqwest::Method::PATCH,
            &format!("/v1/connections/{connection_id}"),
        )
        .json(&UpdateConnectorRequest {
            is_historical_sync: true,
            paused: false,
        })
        .send()
        .await?;

    receive_api_response(res).await
}

#[derive(Serialize)]
struct UpdateConnectorRequest {
    is_historical_sync: bool,
    paused: bool,
}

async fn get_connector(
    client: &Client,
    connection_id: &str,
) -> anyhow::Result<ConnectorResponseV1> {
    log::info!("get_connection");

    let res = client
        .request(
            reqwest::Method::GET,
            &format!("/v1/connections/{connection_id}"),
        )
        .send()
        .await?;

    receive_api_response(res).await
}

async fn list_connectors(client: &Client) -> anyhow::Result<ConnectorList> {
    log::info!("list_connectors");

    let res = client
        .request(reqwest::Method::GET, "/v1/connections")
        .send()
        .await?;

    receive_api_response(res).await
}

async fn list_connectors_of_group(
    client: &Client,
    group_id: &str,
) -> anyhow::Result<ConnectorList> {
    log::info!("list_connection_of_group");

    let res = client
        .request(
            reqwest::Method::GET,
            &format!("/v1/groups/{group_id}/connections"),
        )
        .send()
        .await?;

    receive_api_response(res).await
}

#[derive(Debug, Deserialize)]
struct ConnectorList {
    items: Vec<ConnectorResponse>,
}

#[derive(Debug, Deserialize)]
struct ConnectorResponse {
    id: String,
    service: String,
    schema: String,
    paused: bool,
    status: ConnectorStatusResponse,
    sync_frequency: u64,
    group_id: String,
    connected_by: String,
    created_at: String,
    pause_after_trial: bool,
    schedule_type: String,
    // config: Option<ConnectorResponseConfig>,
    daily_sync_time: Option<String>,
    succeeded_at: Option<String>,
    // setup_tests: Option<Vec<SetupTestResultResponse>>,
    // source_sync_details: Option<"ConnectorResponseSourceSyncDetails">,
    // service_version: Option<int>,
    failed_at: Option<String>,
    // private_link_id: Option<String>,
    // proxy_agent_id: Option<String>,
    // networking_method: Option<ConnectorResponseNetworkingMethod>,
    // connect_card: Option<"ConnectCardResponse">,
    // data_delay_threshold: Option<int>,
    // data_delay_sensitivity: Option<ConnectorResponseDataDelaySensitivity>,
    // connect_card_config: Option<"ConnectCardConfig">,
    // hybrid_deployment_agent_id: Option<String>,
}

async fn delete_connector(client: &Client, connector_id: &str) -> anyhow::Result<()> {
    log::info!("delete_connector");

    let res = client
        .request(
            reqwest::Method::DELETE,
            &format!("/v1/connections/{connector_id}"),
        )
        .send()
        .await?;

    receive_api_response_empty(res).await
}

// --- schema config reload ---

async fn reload_connector_schema_config(
    client: &Client,
    connection_id: &str,
) -> anyhow::Result<StandardConfigResponse> {
    log::info!("reload_connection_schema_config");

    let res = client
        .request(
            reqwest::Method::POST,
            &format!("/v1/connections/{connection_id}/schemas/reload"),
        )
        .json(&ReloadStandardConfigRequest {})
        .send()
        .await?;

    receive_api_response(res).await
}

#[derive(Serialize)]
struct ReloadStandardConfigRequest {}

#[derive(Debug, Deserialize)]
struct StandardConfigResponse {
    schemas: HashMap<String, SchemaConfigResponse>,
    // schema_change_handling: StandardConfigResponseSchemaChangeHandling,
    enable_new_by_default: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct SchemaConfigResponse {
    name_in_destination: String,
    enabled: bool,
    tables: HashMap<String, TableConfigResponse>,
}

#[derive(Debug, Deserialize)]
struct TableConfigResponse {
    name_in_destination: String,
    enabled: bool,
    columns: HashMap<String, ColumnConfigResponse>,
    // enabled_patch_settings: "TableEnabledPatchSettings",
    // sync_mode: Option<TableConfigResponseSyncMode>,
    supports_columns_config: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ColumnConfigResponse {
    name_in_destination: String,
    enabled: bool,
    hashed: bool,
    // enabled_patch_settings: "ColumnEnabledPatchSettings",
    is_primary_key: Option<bool>,
}

// --- schema config update ---

async fn update_connector_schema_config(
    client: &Client,
    connection_id: &str,
    request: &UpdateConnectorSchemaRequest,
) -> anyhow::Result<StandardConfigResponse> {
    log::info!("update_connector_schema_config");

    let res = client
        .request(
            reqwest::Method::PATCH,
            &format!("/v1/connections/{connection_id}/schemas"),
        )
        .json(request)
        .send()
        .await?;

    receive_api_response(res).await
}

#[derive(Serialize)]
struct UpdateConnectorSchemaRequest {
    schema_change_handling: SchemaChangeHandling,
    schemas: HashMap<String, UpdateConnectorSchema>,
}

/// The possible values for the schema_change_handling parameter are as follows:
#[derive(Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum SchemaChangeHandling {
    /// all new schemas, tables, and columns which appear in the source after the initial setup are included in syncs
    AllowAll,
    /// all new schemas and tables which appear in the source after the initial setup are excluded from syncs, but new columns are included
    AllowColumns,
    /// all new schemas, tables, and columns which appear in the source after the initial setup are excluded from syncs
    BlockAll,
}

#[derive(Serialize)]
struct UpdateConnectorSchema {
    enabled: bool,
    tables: HashMap<String, UpdateConnectorTable>,
}

#[derive(Serialize)]
struct UpdateConnectorTable {
    enabled: bool,
    columns: HashMap<String, UpdateConnectorColumn>,
}

#[derive(Serialize)]
struct UpdateConnectorColumn {
    enabled: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    hashed: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    is_primary_key: Option<bool>,
}
