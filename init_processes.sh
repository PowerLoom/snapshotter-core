#!/bin/bash


poetry run python -m snapshotter.snapshotter_id_ping
ret_status=$?

if [ $ret_status -ne 0 ]; then
    echo "Snapshotter identity check failed on protocol smart contract"
    exit 1
fi

# sleep for 30 seconds to allow other services to start
sleep 30

poetry run python -m snapshotter.init_rabbitmq

echo 'starting processes...';
pm2 start pm2.config.js

echo 'started all snapshotter scripts';

pm2 logs --lines 1000
