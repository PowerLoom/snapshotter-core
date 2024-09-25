#!/bin/bash

source .env

# Calculate subnet values based on SLOT_ID
SUBNET_SECOND_OCTET=$((16 + (SLOT_ID / 256) % 240))
SUBNET_THIRD_OCTET=$((SLOT_ID % 256))
export DOCKER_NETWORK_NAME="snapshotter-core-${SLOT_ID}"
export DOCKER_NETWORK_SUBNET="172.${SUBNET_SECOND_OCTET}.${SUBNET_THIRD_OCTET}.0/24"

if [ "$DEVMODE" != "true" ]; then

    if [ -z "$OVERRIDE_DEFAULTS" ]; then
        echo "setting default values..."
        export PROST_RPC_URL="https://rpc-prost1m.powerloom.io"
        export PROTOCOL_STATE_CONTRACT="0xE88E5f64AEB483d7057645326AdDFA24A3B312DF"
        export DATA_MARKET_CONTRACT="0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"
        export PROST_CHAIN_ID="11169"
    fi

    export DOCKER_NETWORK_NAME="snapshotter-core-${SLOT_ID}"
    export DOCKER_NETWORK_SUBNET="172.${SUBNET_SECOND_OCTET}.${SUBNET_THIRD_OCTET}.0/24"
fi

echo "testing before build..."

if [ -z "$SOURCE_RPC_URL" ]; then
    echo "RPC URL not found, please set this in your .env!"
    exit 1
fi

if [ -z "$SIGNER_ACCOUNT_ADDRESS" ]; then
    echo "SIGNER_ACCOUNT_ADDRESS not found, please set this in your .env!"
    exit 1
fi

if [ -z "$SIGNER_ACCOUNT_PRIVATE_KEY" ]; then
    echo "SIGNER_ACCOUNT_PRIVATE_KEY not found, please set this in your .env!"
    exit 1
fi

if [ -z "$DOCKER_NETWORK_SUBNET" ]; then
    echo "DOCKER_NETWORK_SUBNET not found, please set this in your .env!"
    exit 1
fi

echo "DOCKER NETWORK SUBNET: ${DOCKER_NETWORK_SUBNET}"
echo "DOCKER NETWORK NAME: ${DOCKER_NETWORK_NAME}"

echo "Found SOURCE RPC URL ${SOURCE_RPC_URL}"
echo "Found SIGNER ACCOUNT ADDRESS ${SIGNER_ACCOUNT_ADDRESS}"

[ -n "$PROST_RPC_URL" ] && echo "Found PROST_RPC_URL ${PROST_RPC_URL}"
[ -n "$PROST_CHAIN_ID" ] && echo "Found PROST_CHAIN_ID ${PROST_CHAIN_ID}"
[ -n "$IPFS_URL" ] && echo "Found IPFS_URL ${IPFS_URL}"
[ -n "$PROTOCOL_STATE_CONTRACT" ] && echo "Found PROTOCOL_STATE_CONTRACT ${PROTOCOL_STATE_CONTRACT}"
[ -n "$WEB3_STORAGE_TOKEN" ] && echo "Found WEB3_STORAGE_TOKEN ${WEB3_STORAGE_TOKEN}"
[ -n "$SLACK_REPORTING_URL" ] && echo "Found SLACK_REPORTING_URL ${SLACK_REPORTING_URL}"
[ -n "$POWERLOOM_REPORTING_URL" ] && echo "Found POWERLOOM_REPORTING_URL ${POWERLOOM_REPORTING_URL}"

if [ -z "$CORE_API_PORT" ]; then
    export CORE_API_PORT=8002
    echo "CORE_API_PORT not found in .env, setting to default value ${CORE_API_PORT}"
else
    echo "Found CORE_API_PORT ${CORE_API_PORT}"
fi

if [ -z "$LOCAL_COLLECTOR_PORT" ]; then
    export LOCAL_COLLECTOR_PORT=50051
    echo "LOCAL_COLLECTOR_PORT not found in .env, setting to default value ${LOCAL_COLLECTOR_PORT}"
else
    echo "Found LOCAL_COLLECTOR_PORT ${LOCAL_COLLECTOR_PORT}"
fi

# Get the first command line argument
ARG1=${1:-yes_collector}

if [ "$DEVMODE" = "true" ]; then
    export SNAPSHOTTER_COLLECTOR_IMAGE="snapshotter-lite-local-collector"
    export SNAPSHOTTER_IMAGE="snapshotter-core"
else
    #fetch current git branch name
    GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

    echo "Current branch is ${GIT_BRANCH}"

    #if on main git branch, set image_tag to latest or use the branch name
    export IMAGE_TAG=$([ "$GIT_BRANCH" = "dockerify" ] && echo "dockerify" || echo "latest")

    echo "Building image with tag ${IMAGE_TAG}"

    export SNAPSHOTTER_COLLECTOR_IMAGE="ghcr.io/powerloom/snapshotter-lite-local-collector:${IMAGE_TAG}"
    export SNAPSHOTTER_IMAGE="ghcr.io/powerloom/snapshotter-core:${IMAGE_TAG}"
fi

PROFILES=""
[ "$IPFS_URL" = "/dns/ipfs/tcp/5001" ] && PROFILES="$PROFILES --profile ipfs"
[ "$ARG1" = "yes_collector" ] && PROFILES="$PROFILES --profile local-collector"

if command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
else
    echo 'docker compose not found, trying to see if compose exists within docker'
    COMPOSE_CMD="docker compose"
fi

if [ "$DEVMODE" = "false" ]; then
    $COMPOSE_CMD -f docker-compose.yaml $PROFILES pull
fi

# $COMPOSE_CMD -f docker-compose.yaml $PROFILES up -V --abort-on-container-exit
$COMPOSE_CMD -f docker-compose.yaml $PROFILES down --volumes