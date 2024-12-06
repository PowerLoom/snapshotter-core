#!/bin/bash

source .env

echo 'populating setting from environment values...';

if [ -z "$SOURCE_RPC_URL" ]; then
    echo "RPC URL not found, please set this in your .env!";
    exit 1;
fi

if [ -z "$SIGNER_ACCOUNT_ADDRESS" ]; then
    echo "SIGNER_ACCOUNT_ADDRESS not found, please set this in your .env!";
    exit 1;
fi

if [ -z "$PROST_RPC_URL" ]; then
    echo "PROST_RPC_URL not found, please set this in your .env!";
    exit 1;
fi

if [ -z "$SLOT_ID" ]; then
    echo "SLOT_ID not found, please set this in your .env!";
    exit 1;
fi

if [ -z "$PROTOCOL_STATE_CONTRACT" ]; then
    echo "PROTOCOL_STATE_CONTRACT not found, please set this in your .env!";
    exit 1;
fi

if [ -z "$SIGNER_ACCOUNT_PRIVATE_KEY" ]; then
    echo "SIGNER_ACCOUNT_PRIVATE_KEY not found, please set this in your .env!";
    exit 1;
fi


echo "Found SOURCE RPC URL ${SOURCE_RPC_URL}";

echo "Found SIGNER ACCOUNT ADDRESS ${SIGNER_ACCOUNT_ADDRESS}";

if [ "$IPFS_URL" ]; then
    echo "Found IPFS_URL ${IPFS_URL}";
fi


if [ "$SLACK_REPORTING_URL" ]; then
    echo "Found SLACK_REPORTING_URL ${SLACK_REPORTING_URL}";
fi

if [ "$DATA_MARKET_CONTRACT" ]; then
    echo "Found DATA_MARKET_CONTRACT ${DATA_MARKET_CONTRACT}";
fi

if [ "$POWERLOOM_REPORTING_URL" ]; then
    echo "Found POWERLOOM_REPORTING_URL ${POWERLOOM_REPORTING_URL}";
fi

if [ "$TELEGRAM_REPORTING_URL" ]; then
    echo "Found TELEGRAM_REPORTING_URL ${TELEGRAM_REPORTING_URL}";
fi

if [ "$TELEGRAM_CHAT_ID" ]; then
    echo "Found TELEGRAM_CHAT_ID ${TELEGRAM_CHAT_ID}";
fi


if [ "$NAMESPACE" ]; then
    echo "Found NAMESPACE ${NAMESPACE}";
fi

if [ "$REDIS_HOST" ]; then
    echo "Found REDIS_HOST ${REDIS_HOST}";
fi

if [ "$REDIS_PORT" ]; then
    echo "Found REDIS_PORT ${REDIS_PORT}";
fi

if [ "$REDIS_PASSWORD" ]; then
    echo "Found REDIS_PASSWORD. Not echoing back.";
fi

if [ "$IPFS_S3_CONFIG_ENABLED" = "true" ]; then
    echo "Found IPFS_S3_CONFIG_ENABLED ${IPFS_S3_CONFIG_ENABLED}";
    export ipfs_s3_config_enabled="${IPFS_S3_CONFIG_ENABLED}";
    export ipfs_s3_config_endpoint_url="${IPFS_S3_CONFIG_ENDPOINT_URL}";
    export ipfs_s3_config_bucket_name="${IPFS_S3_CONFIG_BUCKET_NAME}";
    export ipfs_s3_config_access_key="${IPFS_S3_CONFIG_ACCESS_KEY}";
    export ipfs_s3_config_secret_key="${IPFS_S3_CONFIG_SECRET_KEY}";
else
    export ipfs_s3_config_enabled="false";
    export ipfs_s3_config_endpoint_url="";
    export ipfs_s3_config_bucket_name="";
    export ipfs_s3_config_access_key="";
    export ipfs_s3_config_secret_key="";
fi


cp config/projects.example.json config/projects.json
cp config/settings.example.json config/settings.json
cp config/auth_settings.example.json config/auth_settings.json
cp config/aggregator.example.json config/aggregator.json

export namespace="${NAMESPACE:-namespace_hash}"
export ipfs_url="${IPFS_URL:-}"
export ipfs_api_key="${IPFS_API_KEY:-}"
export ipfs_api_secret="${IPFS_API_SECRET:-}"
export local_collector_port="${LOCAL_COLLECTOR_PORT:-50051}"
export slack_reporting_url="${SLACK_REPORTING_URL:-}"
export powerloom_reporting_url="${POWERLOOM_REPORTING_URL:-}"
export telegram_reporting_url="${TELEGRAM_REPORTING_URL:-}"
export telegram_chat_id="${TELEGRAM_CHAT_ID:-}"


# If IPFS_URL is empty, clear IPFS API key and secret
if [ -z "$IPFS_URL" ]; then
    ipfs_api_key=""
    ipfs_api_secret=""
fi

echo "Using Namespace: ${namespace}"
echo "Using Prost RPC URL: ${PROST_RPC_URL}"
echo "Using IPFS URL: ${ipfs_url}"
echo "Using IPFS API KEY: ${ipfs_api_key}"
echo "Using protocol state contract: ${PROTOCOL_STATE_CONTRACT}"
echo "Using data market contract: ${DATA_MARKET_CONTRACT}"
echo "Using slack reporting url: ${slack_reporting_url}"
echo "Using powerloom reporting url: ${powerloom_reporting_url}"
echo "Using telegram reporting url: ${telegram_reporting_url}"
echo "Using telegram chat id: ${telegram_chat_id}"
echo "Using REDIS_HOST: ${REDIS_HOST}"
echo "Using REDIS_PORT: ${REDIS_PORT}"

sed -i'.backup' "s#relevant-namespace#$namespace#" config/settings.json

sed -i'.backup' "s#account-address#$SIGNER_ACCOUNT_ADDRESS#" config/settings.json
sed -i'.backup' "s#slot-id#$SLOT_ID#" config/settings.json

sed -i'.backup' "s#https://rpc-url#$SOURCE_RPC_URL#" config/settings.json

sed -i'.backup' "s#https://prost-rpc-url#$PROST_RPC_URL#" config/settings.json

sed -i'.backup' "s#ipfs-writer-url#$ipfs_url#" config/settings.json
sed -i'.backup' "s#ipfs-writer-key#$ipfs_api_key#" config/settings.json
sed -i'.backup' "s#ipfs-writer-secret#$ipfs_api_secret#" config/settings.json

sed -i'.backup' "s#ipfs-reader-url#$ipfs_url#" config/settings.json
sed -i'.backup' "s#ipfs-reader-key#$ipfs_api_key#" config/settings.json
sed -i'.backup' "s#ipfs-reader-secret#$ipfs_api_secret#" config/settings.json

sed -i'.backup' "s#protocol-state-contract#$PROTOCOL_STATE_CONTRACT#" config/settings.json
sed -i'.backup' "s#data-market-contract#$DATA_MARKET_CONTRACT#" config/settings.json
sed -i'.backup' "s#https://slack-reporting-url#$slack_reporting_url#" config/settings.json

sed -i'.backup' "s#https://powerloom-reporting-url#$powerloom_reporting_url#" config/settings.json

sed -i'.backup' "s#signer-account-private-key#$SIGNER_ACCOUNT_PRIVATE_KEY#" config/settings.json

sed -i'.backup' "s#local-collector-port#$local_collector_port#" config/settings.json

sed -i'.backup' "s#https://telegram-reporting-url#$telegram_reporting_url#" config/settings.json
sed -i'.backup' "s#telegram-chat-id#$telegram_chat_id#" config/settings.json

sed -i'.backup' "s#redis-host#$REDIS_HOST#" config/settings.json
sed -i'.backup' "s#\"redis-port\"#$REDIS_PORT#" config/settings.json
if [ "$REDIS_PASSWORD" ]; then
    sed -i'.backup' "s#\"redis-password\"#\"$REDIS_PASSWORD\"#" config/settings.json
else
    sed -i'.backup' "s#\"redis-password\"#null#" config/settings.json
fi

sed -i'.backup' "s#ipfs-s3-config-enabled#$ipfs_s3_config_enabled#" config/settings.json
sed -i'.backup' "s#ipfs-s3-endpoint-url#$ipfs_s3_config_endpoint_url#" config/settings.json
sed -i'.backup' "s#ipfs-s3-bucket-name#$ipfs_s3_config_bucket_name#" config/settings.json
sed -i'.backup' "s#ipfs-s3-access-key#$ipfs_s3_config_access_key#" config/settings.json
sed -i'.backup' "s#ipfs-s3-secret-key#$ipfs_s3_config_secret_key#" config/settings.json

# Add the same replacements for auth_settings.json
sed -i'.backup' "s#redis-host#$REDIS_HOST#" config/auth_settings.json
sed -i'.backup' "s#\"redis-port\"#$REDIS_PORT#" config/auth_settings.json
if [ "$REDIS_PASSWORD" ]; then
    sed -i'.backup' "s#\"redis-password\"#\"$REDIS_PASSWORD\"#" config/auth_settings.json
else
    sed -i'.backup' "s#\"redis-password\"#null#" config/auth_settings.json
fi


# Set default rate limits
SOURCE_RPC_RATE_LIMIT="${SOURCE_RPC_RATE_LIMIT:-10}"
PROST_RPC_RATE_LIMIT="${PROST_RPC_RATE_LIMIT:-10}"

echo "Using SOURCE RPC Rate Limit: ${SOURCE_RPC_RATE_LIMIT}"
echo "Using PROST RPC Rate Limit: ${PROST_RPC_RATE_LIMIT}"

sed -i'.backup' "s/\"source-rpc-rate-limit\"/$SOURCE_RPC_RATE_LIMIT/" config/settings.json

sed -i'.backup' "s/\"prost-rpc-rate-limit\"/$PROST_RPC_RATE_LIMIT/" config/settings.json

echo 'settings has been populated!'
