#!/bin/bash

# check if .env exists
if [ ! -f .env ]; then
    echo ".env file not found, please create one!"
    echo "creating .env file..."
    cp env.example .env

    # ask user for SOURCE_RPC_URL and replace it in .env
    if [ -z "$SOURCE_RPC_URL" ]; then
        read -p "Enter SOURCE_RPC_URL: " SOURCE_RPC_URL
        sed -i'.backup' "s#<source-rpc-url>#$SOURCE_RPC_URL#" .env
    fi

    # ask user for SIGNER_ACCOUNT_ADDRESS and replace it in .env
    if [ -z "$SIGNER_ACCOUNT_ADDRESS" ]; then
        read -p "Enter SIGNER_ACCOUNT_ADDRESS: " SIGNER_ACCOUNT_ADDRESS
        sed -i'.backup' "s#<signer-account-address>#$SIGNER_ACCOUNT_ADDRESS#" .env
    fi

    # ask user for SIGNER_ACCOUNT_PRIVATE_KEY and replace it in .env
    if [ -z "$SIGNER_ACCOUNT_PRIVATE_KEY" ]; then
        read -p "Enter SIGNER_ACCOUNT_PRIVATE_KEY: " SIGNER_ACCOUNT_PRIVATE_KEY
        sed -i'.backup' "s#<signer-account-private-key>#$SIGNER_ACCOUNT_PRIVATE_KEY#" .env
    fi

    # ask user for SLOT_ID and replace it in .env
    if [ -z "$SLOT_ID" ]; then
        read -p "Enter Your SLOT_ID (NFT_ID): " SLOT_ID
        sed -i'.backup' "s#<slot-id>#$SLOT_ID#" .env
    fi

    # ask user for TELEGRAM_CHAT_ID and replace it in .env
    if [ -z "$TELEGRAM_CHAT_ID" ]; then
        read -p "Enter Your TELEGRAM_CHAT_ID (Optional, leave blank to skip.): " TELEGRAM_CHAT_ID
        sed -i'.backup' "s#<telegram-chat-id>#$TELEGRAM_CHAT_ID#" .env
    fi
fi

source .env

# Calculate subnet values based on SLOT_ID
SUBNET_SECOND_OCTET=$((16 + (SLOT_ID / 256) % 240))
SUBNET_THIRD_OCTET=$((SLOT_ID % 256))

if [ "$DEVMODE" = "true" ]; then
    SUBNET_SECOND_OCTET=$((32 + (SLOT_ID / 256) % 224))
    SUBNET_THIRD_OCTET=$((SLOT_ID % 256))
    export DOCKER_NETWORK_NAME="snapshotter-core-${SLOT_ID}"
    export DOCKER_NETWORK_SUBNET="172.${SUBNET_SECOND_OCTET}.${SUBNET_THIRD_OCTET}.0/24"
fi 

if [ -z "$OVERRIDE_DEFAULTS" ] && [ "$DEVMODE" != "true" ]; then
    echo "setting default values..."
    export PROST_RPC_URL="https://rpc-prost1m.powerloom.io"
    export PROTOCOL_STATE_CONTRACT="0xE88E5f64AEB483d7057645326AdDFA24A3B312DF"
    export DATA_MARKET_CONTRACT="0x0C2E22fe7526fAeF28E7A58c84f8723dEFcE200c"
    export PROST_CHAIN_ID="11169"
    export DOCKER_NETWORK_NAME="snapshotter-core-${SLOT_ID}"
    export DOCKER_NETWORK_SUBNET="172.${SUBNET_SECOND_OCTET}.${SUBNET_THIRD_OCTET}.0/24"
    
    # Test function for subnet calculation
    test_subnet_calculation() {
        local test_slot_id=$1
        local expected_second_octet=$2
        local expected_third_octet=$3

        local test_subnet_second_octet=$((16 + (test_slot_id / 256) % 240))
        local test_subnet_third_octet=$((test_slot_id % 256))

        if [ $test_subnet_second_octet -eq $expected_second_octet ] && [ $test_subnet_third_octet -eq $expected_third_octet ]; then
            echo "Test passed for SLOT_ID $test_slot_id: 172.$test_subnet_second_octet.$test_subnet_third_octet.0/24"
        else
            echo "Test failed for SLOT_ID $test_slot_id: Expected 172.$expected_second_octet.$expected_third_octet.0/24, got 172.$test_subnet_second_octet.$test_subnet_third_octet.0/24"
        fi
    }

    # Run tests
    echo "Running subnet calculation tests..."
    test_subnet_calculation 1 16 1
    test_subnet_calculation 255 16 255
    test_subnet_calculation 256 17 0
    test_subnet_calculation 1000 19 232
    test_subnet_calculation 10000 55 16
    test_subnet_calculation 61439 255 255
    test_subnet_calculation 61440 16 0
    test_subnet_calculation 100000 166 160

    # Add this line to run tests before the main script logic
    [ "$1" = "--test" ] && exit 0
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

# check if ufw command exists
if command -v ufw &> /dev/null; then
    # delete old blanket allow rule
    ufw delete allow $LOCAL_COLLECTOR_PORT &> /dev/null
    if ufw allow from $DOCKER_NETWORK_SUBNET to any port $LOCAL_COLLECTOR_PORT; then
        echo "ufw allow rule added for local collector port ${LOCAL_COLLECTOR_PORT} to allow connections from ${DOCKER_NETWORK_SUBNET}."
    else
        echo "ufw firewall allow rule could not be added for local collector port ${LOCAL_COLLECTOR_PORT}"
        echo "Please attempt to add it manually with the following command with sudo privileges:"
        echo "sudo ufw allow from $DOCKER_NETWORK_SUBNET to any port $LOCAL_COLLECTOR_PORT"
        echo "Then run ./build.sh again."
        exit 1
    fi
else
    echo "ufw command not found, skipping firewall rule addition for local collector port ${LOCAL_COLLECTOR_PORT}."
    echo "If you are on a Linux VPS, please ensure that the port is open for connections from ${DOCKER_NETWORK_SUBNET} manually to ${LOCAL_COLLECTOR_PORT}."
fi

# Get the first command line argument
ARG1=${1:-yes_collector}

if [ "$DEVMODE" = "true" ]; then
    echo "Building local collector..."
    git clone https://github.com/powerloom/snapshotter-lite-local-collector.git --single-branch --branch main
    (cd ./snapshotter-lite-local-collector/ && chmod +x build-docker.sh && ./build-docker.sh)

    echo "Building snapshotter..."
    docker build -t snapshotter-core .

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

$COMPOSE_CMD -f docker-compose.yaml $PROFILES up -V --abort-on-container-exit