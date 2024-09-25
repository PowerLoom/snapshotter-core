#!/bin/bash

# check if .env exists
if [ ! -f .env ]; then
    echo ".env file not found, please create one!";
    echo "creating .env file...";
    cp env.example .env;

    # ask user for SOURCE_RPC_URL and replace it in .env
    if [ -z "$SOURCE_RPC_URL" ]; then
        echo "Enter SOURCE_RPC_URL: ";
        read SOURCE_RPC_URL;
        sed -i'.backup' "s#<source-rpc-url>#$SOURCE_RPC_URL#" .env
    fi

    # ask user for SIGNER_ACCOUNT_ADDRESS and replace it in .env
    if [ -z "$SIGNER_ACCOUNT_ADDRESS" ]; then
        echo "Enter SIGNER_ACCOUNT_ADDRESS: ";
        read SIGNER_ACCOUNT_ADDRESS;
        sed -i'.backup' "s#<signer-account-address>#$SIGNER_ACCOUNT_ADDRESS#" .env
    fi

    # ask user for SIGNER_ACCOUNT_PRIVATE_KEY and replace it in .env
    if [ -z "$SIGNER_ACCOUNT_PRIVATE_KEY" ]; then
        echo "Enter SIGNER_ACCOUNT_PRIVATE_KEY: ";
        read SIGNER_ACCOUNT_PRIVATE_KEY;
        sed -i'.backup' "s#<signer-account-private-key>#$SIGNER_ACCOUNT_PRIVATE_KEY#" .env
    fi

    # ask user for SLOT_ID and replace it in .env
    if [ -z "$SLOT_ID" ]; then
        echo "Enter Your SLOT_ID (NFT_ID): ";
        read SLOT_ID;
        sed -i'.backup' "s#<slot-id>#$SLOT_ID#" .env
    fi

    # ask user for TELEGRAM_CHAT_ID and replace it in .env
    if [ -z "$TELEGRAM_CHAT_ID" ]; then
        echo "Enter Your TELEGRAM_CHAT_ID (Optional, leave blank to skip.): ";
        read TELEGRAM_CHAT_ID;
        sed -i'.backup' "s#<telegram-chat-id>#$TELEGRAM_CHAT_ID#" .env
    fi

fi

source .env

echo "testing before build...";

if [ -z "$SOURCE_RPC_URL" ]; then
    echo "RPC URL not found, please set this in your .env!";
    exit 1;
fi

if [ -z "$SIGNER_ACCOUNT_ADDRESS" ]; then
    echo "SIGNER_ACCOUNT_ADDRESS not found, please set this in your .env!";
    exit 1;
fi

if [ -z "$SIGNER_ACCOUNT_PRIVATE_KEY" ]; then
    echo "SIGNER_ACCOUNT_ADDRESS not found, please set this in your .env!";
    exit 1;
fi

echo "Found SOURCE RPC URL ${SOURCE_RPC_URL}";

echo "Found SIGNER ACCOUNT ADDRESS ${SIGNER_ACCOUNT_ADDRESS}";

if [ "$PROST_RPC_URL" ]; then
    echo "Found PROST_RPC_URL ${PROST_RPC_URL}";
fi

if [ "$PROST_CHAIN_ID" ]; then
    echo "Found PROST_CHAIN_ID ${PROST_CHAIN_ID}";
fi

if [ "$IPFS_URL" ]; then
    echo "Found IPFS_URL ${IPFS_URL}";
fi

if [ "$PROTOCOL_STATE_CONTRACT" ]; then
    echo "Found PROTOCOL_STATE_CONTRACT ${PROTOCOL_STATE_CONTRACT}";
fi


if [ "$WEB3_STORAGE_TOKEN" ]; then
    echo "Found WEB3_STORAGE_TOKEN ${WEB3_STORAGE_TOKEN}";
fi

if [ "$SLACK_REPORTING_URL" ]; then
    echo "Found SLACK_REPORTING_URL ${SLACK_REPORTING_URL}";
fi

if [ "$POWERLOOM_REPORTING_URL" ]; then
    echo "Found POWERLOOM_REPORTING_URL ${POWERLOOM_REPORTING_URL}";
fi

if [ -z "$LOCAL_COLLECTOR_PORT" ]; then
    export LOCAL_COLLECTOR_PORT=50051;
    echo "LOCAL_COLLECTOR_PORT not found in .env, setting to default value ${LOCAL_COLLECTOR_PORT}";
else
    echo "Found LOCAL_COLLECTOR_PORT ${LOCAL_COLLECTOR_PORT}";
fi

if [ -z "$CORE_API_PORT" ]; then
    export CORE_API_PORT=8002;
    echo "CORE_API_PORT not found in .env, setting to default value ${CORE_API_PORT}";
else
    echo "Found CORE_API_PORT ${CORE_API_PORT}";
fi

if [ -z "$REDIS_HOST" ]; then
    export REDIS_HOST="redis";
    echo "REDIS_HOST not found in .env, setting to default value ${REDIS_HOST}";
else
    echo "Found REDIS_HOST ${REDIS_HOST}";
fi

if [ -z "$REDIS_PORT" ]; then
    export REDIS_PORT=6379;
    echo "REDIS_PORT not found in .env, setting to default value ${REDIS_PORT}";
else
    echo "Found REDIS_PORT ${REDIS_PORT}";
fi

if [ -z "$REDIS_PASSWORD" ]; then
    echo "REDIS_PASSWORD not found in .env, skipping REDIS_PASSWORD"
else
    echo "Found REDIS_PASSWORD ${REDIS_PASSWORD}"
fi
# We're now using the range 172.32.0.0 to 172.255.255.0 for our subnets, which avoids the commonly used 172.17.0.0/16 range.
# This approach provides 57,344 unique subnets before wrapping around, which should be sufficient for most use cases.
# The calculation ensures each slot ID gets a unique subnet within this range.
SUBNET_SECOND_OCTET=$((32 + (SLOT_ID / 256) % 224))
SUBNET_THIRD_OCTET=$((SLOT_ID % 256))
export DOCKER_NETWORK_NAME="snapshotter-full-v2-${SLOT_ID}"
export DOCKER_NETWORK_SUBNET="172.${SUBNET_SECOND_OCTET}.${SUBNET_THIRD_OCTET}.0/24"

echo "Selected DOCKER_NETWORK_NAME: ${DOCKER_NETWORK_NAME}"
echo "Selected DOCKER_NETWORK_SUBNET: ${DOCKER_NETWORK_SUBNET}"

# Test function for subnet calculation
test_subnet_calculation() {
    local test_slot_id=$1
    local expected_second_octet=$2
    local expected_third_octet=$3

    SLOT_ID=$test_slot_id
    SUBNET_SECOND_OCTET=$((32 + (SLOT_ID / 256) % 224))
    SUBNET_THIRD_OCTET=$((SLOT_ID % 256))

    if [ $SUBNET_SECOND_OCTET -eq $expected_second_octet ] && [ $SUBNET_THIRD_OCTET -eq $expected_third_octet ]; then
        echo "Test passed for SLOT_ID $test_slot_id: 172.$SUBNET_SECOND_OCTET.$SUBNET_THIRD_OCTET.0/24"
    else
        echo "Test failed for SLOT_ID $test_slot_id: Expected 172.$expected_second_octet.$expected_third_octet.0/24, got 172.$SUBNET_SECOND_OCTET.$SUBNET_THIRD_OCTET.0/24"
    fi
}

# Run tests
echo "Running subnet calculation tests..."
test_subnet_calculation 1 32 1
test_subnet_calculation 255 32 255
test_subnet_calculation 256 33 0
test_subnet_calculation 1000 35 232
test_subnet_calculation 10000 71 16
test_subnet_calculation 57343 255 255
test_subnet_calculation 57344 32 0
test_subnet_calculation 100000 182 160


# Add this line to run tests before the main script logic
[ "$1" = "--test" ] && exit 0
# check if ufw command exists
if [ -x "$(command -v ufw)" ]; then
    # delete old blanket allow rule
    ufw delete allow $LOCAL_COLLECTOR_PORT >> /dev/null
    ufw allow from $DOCKER_NETWORK_SUBNET to any port $LOCAL_COLLECTOR_PORT
    if [ $? -eq 0 ]; then
        echo "ufw allow rule added for local collector port ${LOCAL_COLLECTOR_PORT} to allow connections from ${DOCKER_NETWORK_SUBNET}.\n"
    else
            echo "ufw firewall allow rule could not added for local collector port ${LOCAL_COLLECTOR_PORT} \
Please attempt to add it manually with the following command with sudo privileges: \
sudo ufw allow from $DOCKER_NETWORK_SUBNET to any port $LOCAL_COLLECTOR_PORT \
Then run ./build.sh again."
        # exit script if ufw rule not added
        exit 1
    fi
else
    echo "ufw command not found, skipping firewall rule addition for local collector port ${LOCAL_COLLECTOR_PORT}. \
If you are on a Linux VPS, please ensure that the port is open for connections from ${DOCKER_NETWORK_SUBNET} manually to ${LOCAL_COLLECTOR_PORT}."
fi
# setting up git submodules
# git submodule update --init --recursive
git clone https://github.com/powerloom/snapshotter-lite-local-collector.git --single-branch --branch main
cd ./snapshotter-lite-local-collector/ && chmod +x build-docker.sh && ./build-docker.sh;
cd ../;

docker build -t snapshotter-full-v2 .

echo "building...";

PROFILES=""
if [ "$IPFS_URL" == "/dns/ipfs/tcp/5001" ]; then
    PROFILES="$PROFILES --profile ipfs"
fi
if [ "$REDIS_HOST" == "redis" ]; then
    PROFILES="$PROFILES --profile local-redis"
fi
if ! [ -x "$(command -v docker-compose)" ]; then
    echo 'docker compose not found, trying to see if compose exists within docker';  
    docker compose -f docker-compose-dev.yaml $PROFILES up -V --abort-on-container-exit
else
    docker-compose -f docker-compose-dev.yaml $PROFILES up -V --abort-on-container-exit
fi
