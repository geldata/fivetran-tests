_default:
    just --list

generate-python:
    openapi-python-client generate \
        --path ./fivetran_openapi_v1.json \
        --output-path ./fivetran_client \
        --overwrite
