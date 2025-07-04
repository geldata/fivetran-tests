mod fivetran;
mod postgres;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::{env, path};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    log::info!("cleanup_old");
    fivetran::cleanup_old().await?;

    let (postgres, gel_server) = tokio::join!(start_postgres(), start_gel_server(),);
    let gel_addr_local = SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        gel_server.info.port,
    );
    // let postgres = start_postgres().await;
    // let gel_addr_local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5656);

    log::debug!("postgres = {:?}", postgres);
    log::debug!("gel_server = {:?}", gel_server.info);

    let postgres_bore = init_bore(postgres.tcp_address).await?;
    let postgres_addr_pub = get_bore_pub_addr(&postgres_bore)?;

    let gel_server_bore = init_bore(gel_addr_local).await?;
    let gel_addr_pub = get_bore_pub_addr(&gel_server_bore)?;

    log::info!("postgres_addr_pub = {postgres_addr_pub:?}");
    log::info!("gel_addr_pub = {gel_addr_pub:?}");

    // run bores until ctrl-c or timeout
    tokio::spawn(async {
        tokio::select! {
            r = run_bores(postgres_bore, gel_server_bore) => {
                r.unwrap();
            }
            _ = tokio::signal::ctrl_c() => {},
            // _ = tokio::time::sleep(tokio::time::Duration::from_secs(1000)) => {}
        }
    });

    // run tests
    log::info!("setting up fivetran sync");
    let objects = fivetran::setup_sync(postgres_addr_pub, gel_addr_pub).await?;
    // tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    fivetran::cleanup(&objects).await?;

    // validating transferred data
    log::info!("validating synced data");
    postgres::validate_data(postgres.tcp_address).await?;
    log::info!("sync tests passed");

    // stop servers
    drop(postgres);
    drop(gel_server);
    Ok(())
}

async fn init_bore(local_addr: SocketAddr) -> anyhow::Result<bore_cli::client::Client> {
    let bore_server_ip = env::var("BORE_SERVER_IP")?;
    let bore_server_secret = env::var("BORE_SERVER_SECRET")?;

    bore_cli::client::Client::new(
        &local_addr.ip().to_string(),
        local_addr.port(),
        &bore_server_ip,
        0,
        Some(&bore_server_secret),
    )
    .await
}

fn get_bore_pub_addr(client: &bore_cli::client::Client) -> anyhow::Result<SocketAddr> {
    let bore_server_ip = env::var("BORE_SERVER_IP")?;
    let ip = std::net::IpAddr::from_str(&bore_server_ip)?;
    Ok(SocketAddr::new(ip, client.remote_port()))
}

async fn run_bores(
    pg_bore: bore_cli::client::Client,
    gel_bore: bore_cli::client::Client,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio::try_join!(pg_bore.listen(), gel_bore.listen())?;
    Ok(())
}

async fn start_postgres() -> gel_pg_captive::PostgresProcess {
    tokio::task::spawn_blocking(|| {
        gel_pg_captive::PostgresBuilder::new()
            .auth(gel_auth::AuthType::Trust)
            .with_automatic_mode(gel_pg_captive::Mode::TcpSsl)
            .with_automatic_bin_path()
            .unwrap()
            .build()
            .unwrap()
    })
    .await
    .unwrap()
}

async fn start_gel_server() -> gel_captive::ServerProcess {
    let server = tokio::task::spawn_blocking(|| gel_captive::ServerBuilder::new().start())
        .await
        .unwrap();

    // apply schema
    server.apply_schema(&path::PathBuf::from_str("./dbschema").unwrap());

    // run setup
    let status = server
        .cli()
        .arg("query")
        .arg("--file")
        .arg("dbschema/setup.edgeql")
        .status()
        .unwrap();
    assert!(status.success());

    server
}
