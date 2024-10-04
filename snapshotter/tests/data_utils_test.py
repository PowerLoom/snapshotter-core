import json
import os
from typing import List
from unittest.mock import patch

import pytest
from fakeredis import FakeAsyncRedis
from pytest_asyncio import fixture as async_fixture
from web3 import AsyncWeb3
from web3.providers.async_rpc import AsyncHTTPProvider

from snapshotter.settings.config import settings
from snapshotter.utils.data_utils import get_project_finalized_cid
from snapshotter.utils.data_utils import get_project_finalized_cids_bulk
from snapshotter.utils.models.settings_model import RateLimitConfig
from snapshotter.utils.models.settings_model import RPCConfigFull
from snapshotter.utils.models.settings_model import RPCNodeConfig
from snapshotter.utils.redis.redis_keys import project_finalized_data_zset
from snapshotter.utils.rpc import RpcHelper

TEST_RPC_CONFIG = RPCConfigFull(
    full_nodes=[
        RPCNodeConfig(
            url='http://127.0.0.1:8545',
            rate_limit=RateLimitConfig(
                requests_per_second=10,
            ),
        ),
    ],
    connection_limits=settings.rpc.connection_limits,
    retry=settings.rpc.retry,
    semaphore_value=settings.rpc.semaphore_value,
    force_archive_blocks=settings.rpc.force_archive_blocks,
    request_time_out=settings.rpc.request_time_out,
    skip_epoch_threshold_blocks=settings.rpc.skip_epoch_threshold_blocks,
    polling_interval=settings.rpc.polling_interval,
)

# Build ABI and Bytecode Paths
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_MARKET_ABI_PATH = os.path.join(CURRENT_DIR, 'static', 'abi', 'DataMarket.json')
DATA_MARKET_BYTECODE_PATH = os.path.join(CURRENT_DIR, 'static', 'bytecode', 'data_market.json')
PROTOCOL_STATE_ABI_PATH = os.path.join('snapshotter', 'static', 'abis', 'ProtocolContract.json')
PROTOCOL_STATE_BYTECODE_PATH = os.path.join(CURRENT_DIR, 'static', 'bytecode', 'protocol_state.json')
PROXY_ABI_PATH = os.path.join(CURRENT_DIR, 'static', 'abi', 'UUPSUpgradeable.json')
PROXY_BYTECODE_PATH = os.path.join(CURRENT_DIR, 'static', 'bytecode', 'uups_upgradeable.json')
DATA_MARKET_FACTORY_ABI_PATH = os.path.join(CURRENT_DIR, 'static', 'abi', 'DataMarketFactory.json')
DATA_MARKET_FACTORY_BYTECODE_PATH = os.path.join(CURRENT_DIR, 'static', 'bytecode', 'data_market_factory.json')

# Load DataMarket ABI and Bytecode
with open(DATA_MARKET_ABI_PATH, 'r') as abi_file:
    DATA_MARKET_ABI = json.load(abi_file)

with open(DATA_MARKET_BYTECODE_PATH, 'r') as bytecode_file:
    DATA_MARKET_BYTECODE = json.load(bytecode_file)['bytecode']

# Load ProtocolState ABI and Bytecode
with open(PROTOCOL_STATE_ABI_PATH, 'r') as abi_file:
    PROTOCOL_STATE_ABI = json.load(abi_file)

with open(PROTOCOL_STATE_BYTECODE_PATH, 'r') as bytecode_file:
    implementation_bytecode_json = json.load(bytecode_file)
    PROTOCOL_STATE_BYTECODE = implementation_bytecode_json['bytecode']

# Load UUPSUpgradeable ABI and Bytecode
with open(PROXY_ABI_PATH, 'r') as proxy_abi_file:
    PROXY_ABI = json.load(proxy_abi_file)

with open(PROXY_BYTECODE_PATH, 'r') as proxy_bytecode_file:
    proxy_bytecode_json = json.load(proxy_bytecode_file)
    PROXY_BYTECODE = proxy_bytecode_json['bytecode']

# Load DataMarketFactory ABI and Bytecode
with open(DATA_MARKET_FACTORY_ABI_PATH, 'r') as abi_file:
    DATA_MARKET_FACTORY_ABI = json.load(abi_file)

with open(DATA_MARKET_FACTORY_BYTECODE_PATH, 'r') as bytecode_file:
    DATA_MARKET_FACTORY_BYTECODE = json.load(bytecode_file)['bytecode']


@async_fixture(scope='module')
async def web3():
    """Initialize a connection to the local Hardhat node."""
    provider = AsyncHTTPProvider('http://127.0.0.1:8545/')
    w3 = AsyncWeb3(provider)
    # Wait until connected
    connected = await w3.is_connected()
    assert connected, 'Unable to connect to the local Hardhat node.'
    yield w3


@async_fixture(scope='module')
async def snapshot(web3: AsyncWeb3):
    # Take a snapshot of the current state
    snapshot_id = await web3.provider.make_request('evm_snapshot', [])
    print(f'Snapshot created with ID: {snapshot_id}')

    yield snapshot_id['result']

    # Revert to the snapshot after all tests are done
    revert_result = await web3.provider.make_request('evm_revert', [snapshot_id['result']])
    print(f'Snapshot revert result: {revert_result}')

    if not revert_result['result']:
        raise Exception('Snapshot revert failed')


@async_fixture(scope='module')
async def project_id():
    yield 'test_project_id'


@async_fixture(scope='module')
async def epoch_ids():
    yield list(range(1, 11))


@async_fixture(scope='module')
async def proxy_contract(web3: AsyncWeb3, snapshot):
    """Deploy the Proxy contract and return the contract instance."""
    accounts = await web3.eth.accounts
    owner = accounts[0]

    # Deploy the Implementation Contract
    implementation_contract = web3.eth.contract(abi=PROTOCOL_STATE_ABI, bytecode=PROTOCOL_STATE_BYTECODE)
    tx_hash_impl = await implementation_contract.constructor().transact()
    tx_receipt_impl = await web3.eth.wait_for_transaction_receipt(tx_hash_impl)
    implementation_address = tx_receipt_impl['contractAddress']

    # Initialize Proxy Constructor Parameters
    initializer = implementation_contract.encodeABI(fn_name='initialize', args=[web3.to_checksum_address(owner)])

    # Deploy the Proxy Contract
    proxy_contract = web3.eth.contract(abi=PROXY_ABI, bytecode=PROXY_BYTECODE)
    tx_hash_proxy = await proxy_contract.constructor(
        implementation_address,
        initializer,
    ).transact({'from': owner})
    tx_receipt_proxy = await web3.eth.wait_for_transaction_receipt(tx_hash_proxy)
    proxy_address = tx_receipt_proxy['contractAddress']

    proxy_instance = web3.eth.contract(address=proxy_address, abi=PROTOCOL_STATE_ABI)

    # Deploy the DataMarketFactory contract
    factory_contract = web3.eth.contract(abi=DATA_MARKET_FACTORY_ABI, bytecode=DATA_MARKET_FACTORY_BYTECODE)
    tx_hash_factory = await factory_contract.constructor().transact({'from': owner})
    tx_receipt_factory = await web3.eth.wait_for_transaction_receipt(tx_hash_factory)
    factory_address = tx_receipt_factory['contractAddress']

    await proxy_instance.functions.updateDataMarketFactory(factory_address).transact({'from': owner})

    yield proxy_instance


@async_fixture(scope='module')
async def data_market_contract(web3: AsyncWeb3, project_id: str, epoch_ids: List[int], proxy_contract):
    """Deploy the DataMarket contract and return the contract instance."""
    accounts = await web3.eth.accounts
    owner = accounts[0]

    epoch_length = 2  # using 2 because hardhat auto-mines on each tx
    source_chain_block_time = 2
    tx_hash = await proxy_contract.functions.createDataMarket(
        owner,
        epoch_length,
        31337,
        source_chain_block_time * 10000,
        False,
    ).transact({'from': owner})

    tx_receipt = await web3.eth.wait_for_transaction_receipt(tx_hash)
    data_market_created_event = proxy_contract.events.DataMarketCreated().process_receipt(tx_receipt)

    # Extract the DataMarket address from the event
    if data_market_created_event:
        data_market_address = data_market_created_event[0]['args']['dataMarketAddress']
        print(f'DataMarket created at address: {data_market_address}')
    else:
        raise Exception('DataMarketCreated event not found in transaction logs')

    settings.data_market = data_market_address
    deployed_contract = web3.eth.contract(address=data_market_address, abi=DATA_MARKET_ABI)

    await deployed_contract.functions.updateAddresses(
        1,
        [owner],
        [True],
    ).transact({'from': owner})

    await deployed_contract.functions.updateBatchSubmissionWindow(
        1000,
    ).transact({'from': owner})

    await deployed_contract.functions.updateEpochManager(
        owner,
    ).transact({'from': owner})

    current_block_number = await web3.eth.get_block_number()

    for epoch_id in epoch_ids:
        await deployed_contract.functions.releaseEpoch(
            current_block_number,
            current_block_number + 1,
        ).transact({'from': owner})

        cid = 'CID' + project_id + str(epoch_id)
        batch_cid = 'CID' + str(current_block_number)
        await deployed_contract.functions.submitSubmissionBatch(
            batch_cid,
            epoch_id,
            epoch_id,
            [project_id],
            [cid],
            b'\x00' * 32,
        ).transact({'from': owner})

        current_block_number += 2

    yield deployed_contract


@async_fixture(scope='module')
async def mock_redis():
    """Create a FakeAsyncRedis instance for testing."""
    fake_redis = FakeAsyncRedis()
    yield fake_redis
    await fake_redis.close()


@async_fixture(scope='module')
async def rpc_helper(web3):
    """Initialize RpcHelper with the Web3 instance."""
    helper = RpcHelper(TEST_RPC_CONFIG)
    helper._w3 = web3
    await helper.init()
    return helper


@pytest.mark.asyncio(loop_scope='module')
async def test_get_project_finalized_cid_success(proxy_contract, data_market_contract, mock_redis, web3, rpc_helper, project_id: str):
    """
    Test `get_project_finalized_cid` function when CID is found in Redis cache.
    """
    epoch_id = 1
    expected_cid = f'CID{project_id}{epoch_id}'
    new_data_market_address = data_market_contract.address

    # Mock Redis data
    await mock_redis.zadd(f'project_finalized_data_zset:{project_id}', {expected_cid: epoch_id})

    with patch('snapshotter.utils.data_utils.settings.data_market', new_data_market_address):
        cid = await get_project_finalized_cid(
            redis_conn=mock_redis,
            state_contract_obj=proxy_contract,
            rpc_helper=rpc_helper,
            epoch_id=epoch_id,
            project_id=project_id,
        )

        assert cid == expected_cid

    # clean slate redis
    await mock_redis.flushall()


@pytest.mark.asyncio(loop_scope='module')
async def test_get_project_finalized_cid_not_found(proxy_contract, data_market_contract, mock_redis, rpc_helper, project_id):
    """
    Test `get_project_finalized_cid` function when CID is not found in Redis cache
    and needs to be fetched from the blockchain.
    """
    epoch_id = 2
    expected_cid = f'CID{project_id}{epoch_id}'
    new_data_market_address = data_market_contract.address

    cid_data = await mock_redis.zrangebyscore(
        project_finalized_data_zset(project_id),
        epoch_id,
        epoch_id,
    )

    assert not cid_data, 'Data should not be cached in Redis'

    with patch('snapshotter.utils.data_utils.settings.data_market', new_data_market_address):
        cid = await get_project_finalized_cid(
            redis_conn=mock_redis,
            state_contract_obj=proxy_contract,
            rpc_helper=rpc_helper,
            epoch_id=epoch_id,
            project_id=project_id,
        )

        assert cid == expected_cid

        # Verify that the CID was added to Redis
        [stored_cid] = await mock_redis.zrangebyscore(
            project_finalized_data_zset(project_id),
            epoch_id,
            epoch_id,
        )
        assert stored_cid.decode('utf-8') == expected_cid

        # clean slate redis
        await mock_redis.flushall()


@pytest.mark.asyncio(loop_scope='module')
async def test_get_project_finalized_cids_bulk_success(proxy_contract, data_market_contract, mock_redis, rpc_helper, project_id: str, epoch_ids: List[int]):
    """
    Test `get_project_finalized_cids_bulk` function for bulk CID retrieval.
    """
    expected_cids = [f'CID{project_id}{epoch_id}' for epoch_id in epoch_ids]

    # Populate Redis with cached CIDs
    for i in range(len(expected_cids)):
        await mock_redis.zadd(project_finalized_data_zset(project_id), {expected_cids[i]: epoch_ids[i]})

    new_data_market_address = data_market_contract.address

    with patch('snapshotter.utils.data_utils.settings.data_market', new_data_market_address):
        cids = await get_project_finalized_cids_bulk(
            redis_conn=mock_redis,
            state_contract_obj=proxy_contract,
            rpc_helper=rpc_helper,
            epoch_ids=epoch_ids,
            project_id=project_id,
        )

        assert cids == expected_cids

    # Verify that all CIDs are now in Redis
    stored_cids = await mock_redis.zrange(
        project_finalized_data_zset(project_id),
        0,
        -1,
        withscores=True,
    )
    assert len(stored_cids) == len(expected_cids)
    assert all(cid.decode('utf-8') in expected_cids for cid, _ in stored_cids)

    # Clean slate Redis
    await mock_redis.flushall()


@pytest.mark.asyncio(loop_scope='module')
async def test_get_project_finalized_cids_bulk_not_found(proxy_contract, data_market_contract, mock_redis, rpc_helper, project_id: str, epoch_ids: List[int]):
    """
    Test `get_project_finalized_cids_bulk` function when CIDs are not found in Redis cache
    and need to be fetched from the blockchain.
    """
    expected_cids = [f'CID{project_id}{epoch_id}' for epoch_id in epoch_ids]
    new_data_market_address = data_market_contract.address

    # Verify that the CIDs are not in Redis
    cached_cids = await mock_redis.zrange(
        project_finalized_data_zset(project_id),
        0,
        -1,
        withscores=True,
    )
    assert not cached_cids, 'No CIDs should be cached in Redis initially'

    with patch('snapshotter.utils.data_utils.settings.data_market', new_data_market_address):
        cids = await get_project_finalized_cids_bulk(
            redis_conn=mock_redis,
            state_contract_obj=proxy_contract,
            rpc_helper=rpc_helper,
            epoch_ids=epoch_ids,
            project_id=project_id,
        )

        assert cids == expected_cids

        # Verify that all CIDs were added to Redis
        stored_cids = await mock_redis.zrangebyscore(
            project_finalized_data_zset(project_id),
            min(epoch_ids),
            max(epoch_ids),
            withscores=True,
        )
        assert len(stored_cids) == len(expected_cids)
        assert all(cid.decode('utf-8') in expected_cids for cid, _ in stored_cids)
        assert all(int(epoch) in epoch_ids for _, epoch in stored_cids)

    # Clean slate Redis
    await mock_redis.flushall()


@pytest.mark.asyncio(loop_scope='module')
async def test_get_project_finalized_cids_bulk_partial(proxy_contract, data_market_contract, mock_redis, rpc_helper, project_id: str, epoch_ids: List[int]):
    """
    Test `get_project_finalized_cids_bulk` function when only partial data is available in Redis cache
    and the rest needs to be fetched from the blockchain.
    """
    expected_cids = [f'CID{project_id}{epoch_id}' for epoch_id in epoch_ids]
    new_data_market_address = data_market_contract.address

    # Populate Redis with CIDs for every other epoch_id
    redis_mapping = {
        expected_cids[i]: epoch_ids[i]
        for i in range(0, len(epoch_ids), 2)
    }
    await mock_redis.zadd(project_finalized_data_zset(project_id), redis_mapping)

    # Verify that only partial data is in Redis
    cached_cids = await mock_redis.zrange(
        project_finalized_data_zset(project_id),
        0,
        -1,
        withscores=True,
    )
    assert len(cached_cids) == len(epoch_ids) // 2, 'Only half of the CIDs should be cached in Redis initially'

    with patch('snapshotter.utils.data_utils.settings.data_market', new_data_market_address):
        cids = await get_project_finalized_cids_bulk(
            redis_conn=mock_redis,
            state_contract_obj=proxy_contract,
            rpc_helper=rpc_helper,
            epoch_ids=epoch_ids,
            project_id=project_id,
        )

        assert cids == expected_cids

        # Verify that all CIDs were added to Redis
        stored_cids = await mock_redis.zrangebyscore(
            project_finalized_data_zset(project_id),
            min(epoch_ids),
            max(epoch_ids),
            withscores=True,
        )
        assert len(stored_cids) == len(expected_cids)
        assert all(cid.decode('utf-8') in expected_cids for cid, _ in stored_cids)
        assert all(int(epoch) in epoch_ids for _, epoch in stored_cids)

    # Clean slate Redis
    await mock_redis.flushall()
