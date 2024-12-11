import asyncio
import json
from typing import List

import tenacity
from redis import asyncio as aioredis
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential
from web3 import Web3

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.redis.redis_keys import cid_not_found_key
from snapshotter.utils.redis.redis_keys import project_finalized_data_zset
from snapshotter.utils.redis.redis_keys import project_first_epoch_hmap
from snapshotter.utils.redis.redis_keys import source_chain_block_time_key
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.redis.redis_keys import source_chain_id_key
from snapshotter.utils.rpc import RpcHelper

logger = default_logger.bind(module='data_helper')
BATCH_SIZE = 50


def retry_state_callback(retry_state: tenacity.RetryCallState):
    """
    Callback function to handle retry attempts for IPFS cat operation.

    This function logs a warning message when an IPFS cat exception occurs during a retry attempt.

    Args:
        retry_state (tenacity.RetryCallState): The current state of the retry call.

    Returns:
        None
    """
    logger.warning(f'Encountered IPFS cat exception: {retry_state.outcome.exception()}')


async def get_project_finalized_cid(redis_conn: aioredis.Redis, state_contract_obj, rpc_helper, epoch_id, project_id):
    """
    Get the CID of the finalized data for a given project and epoch.

    This function first checks if the epoch is valid for the project. If so, it attempts to retrieve the CID
    from a Redis cache. If not found in the cache, it fetches and caches the CID from the blockchain.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: Contract object for the state contract.
        rpc_helper: Helper object for making RPC calls.
        epoch_id (int): Epoch ID for which to get the CID.
        project_id (str): ID of the project for which to get the CID.

    Returns:
        str: CID of the finalized data for the given project and epoch, or None if not found.
    """

    # Check if the epoch is valid for the project
    project_first_epoch = await get_project_first_epoch(
        redis_conn, state_contract_obj, rpc_helper, project_id,
    )
    if epoch_id < project_first_epoch:
        return None

    # Try to get the CID from Redis cache
    cid_data = await redis_conn.zrangebyscore(
        project_finalized_data_zset(project_id),
        epoch_id,
        epoch_id,
    )
    if cid_data:
        cid = cid_data[0].decode('utf-8')
    else:
        # If not in cache, fetch from blockchain and cache it
        cid, _ = await w3_get_and_cache_finalized_cid(redis_conn, state_contract_obj, rpc_helper, epoch_id, project_id)

    # Return None if CID is None (consensus not yet available) or contains 'null'
    if cid is None or 'null' in cid:
        return None
    return cid


async def get_project_last_finalized_epoch(redis_conn: aioredis.Redis, state_contract_obj, rpc_helper, project_id, use_pending=True):
    """
    Get the last finalized epoch for a given project.
    """
    if use_pending:
        [project_last_finalized_epoch] = await rpc_helper.web3_call(
            tasks=[
                ('lastSequencerFinalizedSnapshot', [Web3.to_checksum_address(settings.data_market), project_id]),
            ],
            contract_addr=state_contract_obj.address,
            abi=state_contract_obj.abi,
        )
    else:
        [project_last_finalized_epoch] = await rpc_helper.web3_call(
            tasks=[
                ('lastFinalizedSnapshot', [Web3.to_checksum_address(settings.data_market), project_id]),
            ],
            contract_addr=state_contract_obj.address,
            abi=state_contract_obj.abi,
        )
    return project_last_finalized_epoch


async def get_project_finalized_cids_bulk(
    redis_conn: aioredis.Redis,
    state_contract_obj,
    rpc_helper: RpcHelper,
    epoch_id_min: int,
    epoch_id_max: int,
    project_id: str,
) -> List[str]:
    """
    Retrieves CIDs for multiple epochs in bulk.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: Contract object for the state contract.
        rpc_helper (RpcHelper): Helper object for making RPC calls.
        epoch_ids (List[int]): List of epoch IDs.
        project_id (str): Project ID.

    Returns:
        List[str]: List of CIDs.
    """
    # Adjust epoch_id_min if it's less than the project's first epoch
    project_first_epoch = await get_project_first_epoch(
        redis_conn, state_contract_obj, rpc_helper, project_id,
    )

    if epoch_id_min < project_first_epoch:
        logger.warning(
            f'Min. Epoch ID: {epoch_id_min} is less than the project first epoch {project_first_epoch}.',
            f'Cannot fetch CIDs for epochs before project first epoch.',
        )
        return None

    epoch_ids_set = set(range(epoch_id_min, epoch_id_max + 1))

    # Check Redis cache for existing CIDs
    cid_data_with_epochs = await redis_conn.zrangebyscore(
        project_finalized_data_zset(project_id),
        min=epoch_id_min,
        max=epoch_id_max,
        withscores=True,
    )
    cid_data_with_epochs = [(cid.decode('utf-8'), int(epoch_id)) for cid, epoch_id in cid_data_with_epochs]
    existing_epochs = set([epoch_id for _, epoch_id in cid_data_with_epochs])
    missing_epochs = list(epoch_ids_set.difference(existing_epochs))

    # batch_web3_contract_calls
    if missing_epochs:
        # Batch fetch CIDs from the blockchain
        missing_cids_with_epochs = await w3_get_and_cache_finalized_cid_bulk(
            redis_conn=redis_conn,
            state_contract_obj=state_contract_obj,
            rpc_helper=rpc_helper,
            epoch_ids=missing_epochs,
            project_id=project_id,
        )

        # Merge existing and missing CIDs
        all_cids_with_epochs = cid_data_with_epochs + missing_cids_with_epochs
        all_cids_with_epochs.sort(key=lambda x: x[1])
    else:
        all_cids_with_epochs = cid_data_with_epochs

    return [cid for cid, _ in all_cids_with_epochs]


@retry(
    reraise=True,
    retry=retry_if_exception_type(Exception),
    wait=wait_random_exponential(multiplier=1, max=10),
    stop=stop_after_attempt(3),
)
async def w3_get_and_cache_finalized_cid(
    redis_conn: aioredis.Redis,
    state_contract_obj,
    rpc_helper: RpcHelper,
    epoch_id,
    project_id,
    # NOTE: Setting it to true for now, but we will need to set it to false once validators are live.
    use_pending: bool = True,
):
    """
    Retrieves the consensus status and the snapshot CID for a given project and epoch.

    This function interacts with the blockchain to get the snapshot status and CID, then caches the result in Redis.
    It supports both legacy v1 and new v2 protocols for consensus status.

    Args:
        redis_conn (aioredis.Redis): Redis connection object
        state_contract_obj: Contract object for the protocol state contract
        rpc_helper: Helper object for making web3 calls
        epoch_id (int): Epoch ID
        project_id (int): Project ID

    Returns:
        Tuple[str, int]: The CID and epoch ID if the consensus status is not PENDING, or the null value and epoch ID if the consensus status is PENDING.
    """

    # Fetch consensus status and CID from the blockchain
    [consensus_status] = await rpc_helper.web3_call(
        tasks=[
            ('snapshotStatus', [Web3.to_checksum_address(settings.data_market), project_id, epoch_id]),
        ],
        contract_addr=state_contract_obj.address,
        abi=state_contract_obj.abi,
    )
    logger.trace(f'consensus status for project {project_id} and epoch {epoch_id} is {consensus_status}')

    # Extract status and CID from the ConsensusStatus struct
    status, cid, timestamp = consensus_status

    # If timestamp is 0, consensus status is not yet available
    if timestamp == 0:
        logger.debug(f'Consensus status not yet available for project {project_id} and epoch {epoch_id}')
        return None, epoch_id

    # Process and cache the result only if we have a valid timestamp
    if cid:
        null_cid = f'null_{epoch_id}'
        if use_pending or status > 0:
            await redis_conn.zadd(
                project_finalized_data_zset(project_id),
                {cid: epoch_id},
            )
            return cid, epoch_id
        else:
            return null_cid, epoch_id
    else:
        # Only cache null if we're sure there's no CID (timestamp > 0 but no CID)
        await redis_conn.zadd(
            project_finalized_data_zset(project_id),
            {null_cid: epoch_id},
        )
        return null_cid, epoch_id


@retry(
    reraise=True,
    retry=retry_if_exception_type(Exception),
    wait=wait_random_exponential(multiplier=1, max=10),
    stop=stop_after_attempt(3),
)
async def w3_get_and_cache_finalized_cid_bulk(
    redis_conn: aioredis.Redis,
    state_contract_obj,
    rpc_helper: RpcHelper,
    epoch_ids: List[int],
    project_id: str,
    # NOTE: Setting it to true for now, but we will need to set it to false once validators are live.
    use_pending: bool = True,
):
    """
    Retrieves and caches the consensus status and snapshot CID for multiple epochs of a given project.

    This function interacts with the blockchain to get the snapshot status for multiple epochs,
    then caches the results in Redis.

    Args:
        redis_conn (aioredis.Redis): Redis connection object
        state_contract_obj: Contract object for the protocol state contract
        rpc_helper (RpcHelper): Helper object for making web3 calls
        epoch_ids (List[int]): List of epoch IDs to fetch
        project_id (str): Project ID

    Returns:
        List[Tuple[str, int]]: List of tuples containing (CID, epoch_id) for each epoch
    """
    try:
        all_results = []

        for i in range(0, len(epoch_ids), BATCH_SIZE):
            batch_epoch_ids = epoch_ids[i:i + BATCH_SIZE]

            # Prepare tasks for batch call
            tasks = [
                ('snapshotStatus', [Web3.to_checksum_address(settings.data_market), project_id, epoch_id])
                for epoch_id in batch_epoch_ids
            ]

            # Make batch call
            batch_results = await rpc_helper.batch_web3_contract_calls(
                tasks=tasks,
                contract_obj=state_contract_obj,
            )
            all_results.extend(batch_results)

        # Process results and prepare for caching
        cids_with_epochs = []
        redis_mapping = {}
        
        for i, epoch_id in enumerate(epoch_ids):
            consensus_status = all_results[i]

            # Extract status and CID from the ConsensusStatus struct
            status, cid, timestamp = consensus_status

            # Skip if consensus status is not yet available
            null_cid = f'null_{epoch_id}'
            if timestamp == 0:
                logger.debug(f'Consensus status not yet available for project {project_id} and epoch {epoch_id}')
                cids_with_epochs.append((null_cid, epoch_id))
                continue

            if cid:
                if use_pending or status > 0:
                    redis_mapping[cid] = epoch_id
                    cids_with_epochs.append((cid, epoch_id))
                else:
                    cids_with_epochs.append((null_cid, epoch_id))
            else:
                # Only cache null if we're sure there's no CID (timestamp > 0 but no CID)
                redis_mapping[null_cid] = epoch_id
                cids_with_epochs.append((null_cid, epoch_id))

        if redis_mapping:
            await redis_conn.zadd(
                project_finalized_data_zset(project_id),
                redis_mapping,
            )

        return cids_with_epochs

    except Exception as e:
        logger.error(f'Error in w3_get_and_cache_finalized_cid_bulk: {str(e)}')
        raise


async def get_project_first_epoch(redis_conn: aioredis.Redis, state_contract_obj, rpc_helper: RpcHelper, project_id):
    """
    Get the first epoch for a given project ID.

    This function first checks a Redis cache for the first epoch. If not found, it fetches the information
    from the blockchain and caches it in Redis for future use.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: Contract object for the state contract.
        rpc_helper: RPC helper object.
        project_id (str): ID of the project.

    Returns:
        int: The first epoch for the given project ID.
    """
    # Try to get the first epoch from Redis cache
    first_epoch_data = await redis_conn.hget(
        project_first_epoch_hmap(),
        project_id,
    )
    if first_epoch_data:
        first_epoch = int(first_epoch_data)
        return first_epoch
    else:
        # If not in cache, fetch from blockchain
        [first_epoch] = await rpc_helper.web3_call(
            tasks=[
                ('projectFirstEpochId', [Web3.to_checksum_address(settings.data_market), project_id]),
            ],
            contract_addr=state_contract_obj.address,
            abi=state_contract_obj.abi,
        )
        logger.debug(f'first epoch for project {project_id} is {first_epoch}')

        # Cache the result if it's not 0
        if first_epoch != 0:
            await redis_conn.hset(
                project_first_epoch_hmap(),
                project_id,
                first_epoch,
            )

        return first_epoch


@retry(
    reraise=True,
    retry=retry_if_exception_type(Exception),
    wait=wait_random_exponential(multiplier=0.5, max=10),
    stop=stop_after_attempt(3),
    before_sleep=retry_state_callback,
)
async def _fetch_file_from_ipfs(ipfs_reader, cid):
    """
    Fetches a file from IPFS using the given IPFS reader and CID.

    This function is decorated with a retry mechanism to handle potential IPFS errors.

    Args:
        ipfs_reader: An IPFS reader object.
        cid: The CID of the file to fetch.

    Returns:
        The contents of the file as bytes.
    """
    return await ipfs_reader.cat(cid)


async def fetch_file_from_ipfs(redis_conn: aioredis.Redis, ipfs_reader, cid):
    """
    Fetches a file from IPFS using the given IPFS reader and CID.

    Uses _fetch_file_from_ipfs under the hood, if it is unable to fetch file from IPFS, it will mark the cid as not found in redis.
    """
    if await redis_conn.get(cid_not_found_key(cid)):
        return dict()
    try:
        data = await _fetch_file_from_ipfs(ipfs_reader, cid)
        return json.loads(data)
    except Exception as e:
        logger.error(f'Error while fetching data from IPFS | CID {cid} | Error: {e}')
        await redis_conn.set(cid_not_found_key(cid), 'true', ex=86400)
        return dict()


async def get_submission_data(redis_conn: aioredis.Redis, cid, ipfs_reader, project_id: str) -> dict:
    """
    Fetches submission data from cache or IPFS.

    This function first attempts to read the data from a local cache. If not found,
    it fetches the data from IPFS and then caches it locally.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        cid (str): IPFS content ID.
        ipfs_reader (ipfshttpclient.client.Client): IPFS client object.
        project_id (str): ID of the project.

    Returns:
        dict: Submission data.
    """
    if not cid or 'null' in cid:
        return dict()

    return await fetch_file_from_ipfs(redis_conn, ipfs_reader, cid)


async def get_submission_data_bulk(
    redis_conn: aioredis.Redis,
    cids: List[str],
    ipfs_reader,
    project_ids: List[str],
    ensure_complete: bool = False,
) -> List[dict]:
    """
    Retrieves submission data for multiple submissions in bulk.

    This function processes the submissions in batches to optimize performance.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        cids (List[str]): List of submission CIDs.
        ipfs_reader: IPFS reader object.
        project_ids (List[str]): List of project IDs.

    Returns:
        List[dict]: List of submission data dictionaries.
    """
    all_snapshot_data = []

    # Process submissions in batches
    for i in range(0, len(cids), BATCH_SIZE):
        batch_cids = cids[i:i + BATCH_SIZE]
        batch_project_ids = project_ids[i:i + BATCH_SIZE]
        batch_snapshot_data = await asyncio.gather(
            *[
                get_submission_data(redis_conn, cid, ipfs_reader, project_id)
                for cid, project_id in zip(batch_cids, batch_project_ids)
            ],
        )

        if ensure_complete:
            missing_cids = [
                cid for cid, data in zip(batch_cids, batch_snapshot_data)
                if data == dict()
            ]
            if missing_cids:
                logger.error(f'Incomplete ipfs data for CIDs: {missing_cids}')
                return []

        all_snapshot_data.extend(batch_snapshot_data)

    return all_snapshot_data


async def get_project_epoch_snapshot(
    redis_conn: aioredis.Redis, state_contract_obj, rpc_helper, ipfs_reader, epoch_id, project_id,
) -> dict:
    """
    Retrieves the epoch snapshot for a given project.

    This function first gets the finalized CID for the given epoch and project,
    then fetches the corresponding submission data.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: State contract object.
        rpc_helper: RPC helper object.
        ipfs_reader: IPFS reader object.
        epoch_id (int): Epoch ID.
        project_id (str): Project ID.

    Returns:
        dict: The epoch snapshot data.
    """
    cid = await get_project_finalized_cid(redis_conn, state_contract_obj, rpc_helper, epoch_id, project_id)
    if cid:
        data = await get_submission_data(redis_conn, cid, ipfs_reader, project_id)
        return data
    else:
        return dict()


async def get_source_chain_id(redis_conn: aioredis.Redis, state_contract_obj, rpc_helper: RpcHelper):
    """
    Retrieves the source chain ID from Redis cache if available, otherwise fetches it from the state contract and caches it in Redis.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: State contract object.
        rpc_helper: RPC helper object.

    Returns:
        int: The source chain ID.
    """
    # Try to get the source chain ID from Redis cache
    source_chain_id_data = await redis_conn.get(
        source_chain_id_key(),
    )
    if source_chain_id_data:
        source_chain_id = int(source_chain_id_data.decode('utf-8'))
        return source_chain_id
    else:
        # If not in cache, fetch from blockchain
        [source_chain_id] = await rpc_helper.web3_call(
            tasks=[
                ('SOURCE_CHAIN_ID', [Web3.to_checksum_address(settings.data_market)]),
            ],
            contract_addr=state_contract_obj.address,
            abi=state_contract_obj.abi,
        )

        # Cache the result in Redis
        await redis_conn.set(
            source_chain_id_key(),
            source_chain_id,
        )
        return source_chain_id


async def get_source_chain_epoch_size(redis_conn: aioredis.Redis, state_contract_obj, rpc_helper: RpcHelper):
    """
    This function retrieves the epoch size of the source chain from the state contract.

    It first checks if the epoch size is cached in Redis. If not, it fetches from the blockchain and caches the result.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: Contract object for the state contract.
        rpc_helper: Helper object for making RPC calls.

    Returns:
        int: The epoch size of the source chain.
    """
    # Try to get the epoch size from Redis cache
    source_chain_epoch_size_data = await redis_conn.get(
        source_chain_epoch_size_key(),
    )
    if source_chain_epoch_size_data:
        source_chain_epoch_size = int(source_chain_epoch_size_data.decode('utf-8'))
        return source_chain_epoch_size
    else:
        # If not in cache, fetch from blockchain
        [source_chain_epoch_size] = await rpc_helper.web3_call(
            tasks=[('EPOCH_SIZE', [Web3.to_checksum_address(settings.data_market)])],
            contract_addr=state_contract_obj.address,
            abi=state_contract_obj.abi,
        )

        # Cache the result in Redis
        await redis_conn.set(
            source_chain_epoch_size_key(),
            source_chain_epoch_size,
        )

        return source_chain_epoch_size


async def get_source_chain_block_time(redis_conn: aioredis.Redis, state_contract_obj, rpc_helper: RpcHelper):
    """
    Get the block time of the source chain.

    This function first checks Redis cache for the block time. If not found, it fetches from the blockchain
    and caches the result.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: Contract object for the state contract.
        rpc_helper: RPC helper object.

    Returns:
        int: Block time of the source chain.
    """
    # Try to get the block time from Redis cache
    source_chain_block_time_data = await redis_conn.get(
        source_chain_block_time_key(),
    )
    if source_chain_block_time_data:
        source_chain_block_time = int(source_chain_block_time_data.decode('utf-8'))
        return source_chain_block_time
    else:
        # If not in cache, fetch from blockchain
        [source_chain_block_time] = await rpc_helper.web3_call(
            tasks=[('SOURCE_CHAIN_BLOCK_TIME', [Web3.to_checksum_address(settings.data_market)])],
            contract_addr=state_contract_obj.address,
            abi=state_contract_obj.abi,
        )
        source_chain_block_time = int(source_chain_block_time / 1e4)

        # Cache the result in Redis
        await redis_conn.set(
            source_chain_block_time_key(),
            source_chain_block_time,
        )

        return source_chain_block_time


async def get_tail_epoch_id(
        redis_conn: aioredis.Redis,
        state_contract_obj,
        rpc_helper,
        current_epoch_id,
        time_in_seconds,
        project_id,
):
    """
    Returns the tail epoch_id and a boolean indicating if tail contains the full time window.

    This function calculates the tail epoch ID based on the current epoch and a time window,
    ensuring it doesn't go below the project's first epoch.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: State contract object.
        rpc_helper: RPC helper object.
        current_epoch_id (int): Current epoch ID.
        time_in_seconds (int): Time window in seconds.
        project_id (str): Project ID.

    Returns:
        Tuple[int, bool]: Tail epoch ID and a boolean indicating if tail contains the full time window.
    """
    # Get necessary chain parameters
    source_chain_epoch_size = await get_source_chain_epoch_size(redis_conn, state_contract_obj, rpc_helper)
    source_chain_block_time = await get_source_chain_block_time(redis_conn, state_contract_obj, rpc_helper)

    # Calculate tail epoch_id
    tail_epoch_id = current_epoch_id - int(time_in_seconds / (source_chain_epoch_size * source_chain_block_time))
    project_first_epoch = await get_project_first_epoch(redis_conn, state_contract_obj, rpc_helper, project_id)

    # Ensure tail_epoch_id is not less than project_first_epoch
    if tail_epoch_id < project_first_epoch:
        tail_epoch_id = project_first_epoch
        return tail_epoch_id, True

    logger.trace(
        'project ID {} tail epoch_id: {} against head epoch ID {} ',
        project_id, tail_epoch_id, current_epoch_id,
    )

    return tail_epoch_id, False


async def get_project_epoch_snapshot_bulk(
        redis_conn: aioredis.Redis,
        state_contract_obj,
        rpc_helper,
        ipfs_reader,
        epoch_id_min: int,
        epoch_id_max: int,
        project_id,
        ensure_complete: bool = False,
):
    """
    Fetches the snapshot data for a given project and epoch range.

    This function retrieves snapshot data in bulk, first checking Redis cache and then
    fetching missing data from the blockchain if necessary.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: State contract object.
        rpc_helper: RPC helper object.
        ipfs_reader: IPFS reader object.
        epoch_id_min (int): Minimum epoch ID to fetch snapshot data for.
        epoch_id_max (int): Maximum epoch ID to fetch snapshot data for.
        project_id: ID of the project to fetch snapshot data for.

    Returns:
        A list of snapshot data for the given project and epoch range.
    """
    cid_data = await get_project_finalized_cids_bulk(
        redis_conn, state_contract_obj, rpc_helper, epoch_id_min, epoch_id_max, project_id,
    )

    cid_data_with_epochs = zip(cid_data, range(epoch_id_min, epoch_id_max + 1))
    # Filter out null CIDs
    valid_cid_data_with_epochs = [
        (cid, epoch_id) for cid, epoch_id in cid_data_with_epochs
        if cid and 'null' not in cid
    ]

    if ensure_complete and len(valid_cid_data_with_epochs) != epoch_id_max - epoch_id_min + 1:
        logger.error(f'Incomplete cids found for project {project_id} from epoch {epoch_id_min} to {epoch_id_max}')
        return []

    # Fetch snapshot data in bulk
    all_snapshot_data = await get_submission_data_bulk(
        redis_conn,
        [cid for cid, _ in valid_cid_data_with_epochs],
        ipfs_reader,
        [project_id] * len(valid_cid_data_with_epochs),
        ensure_complete=ensure_complete,
    )

    return all_snapshot_data


async def get_project_time_series_data(
        start_time: int,
        end_time: int,
        step_seconds: int,
        end_epoch_id: int,
        redis_conn: aioredis.Redis,
        state_contract_obj,
        rpc_helper,
        ipfs_reader,
        project_id,
):
    """
    Returns a list of snapshot data containing equally spaced observations starting with the start_epoch id
    for the given project_id, and including epochs spaced step_seconds apart until the maximum observations has been reached.

    Args:
        observations: Total number of data points to gather
        step_seconds: Time in seconds between each obsveration
        project_last_finalized_epoch: Epoch ID of the last finalized epoch for'project_id'
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: State contract object.
        rpc_helper: RPC helper object.
        ipfs_reader: IPFS reader object.
        project_id: ID of the project to fetch snapshot data for.


    Returns:
        A list of snapshot data objects for the given project_id with a maximum length of the observations param.
    """

    # get metadata for building steps
    [
        source_chain_epoch_size,
        source_chain_block_time,
        project_first_epoch,
    ] = await asyncio.gather(
        get_source_chain_epoch_size(
            redis_conn,
            state_contract_obj,
            rpc_helper,
        ),
        get_source_chain_block_time(
            redis_conn,
            state_contract_obj,
            rpc_helper,
        ),
        get_project_first_epoch(
            redis_conn,
            state_contract_obj,
            rpc_helper,
            project_id,
        ),
    )

    seek_stop_flag = False

    closest_step_time_gap = end_time % step_seconds
    closest_step_timestamp = end_time - closest_step_time_gap
    closest_step_epoch_id = end_epoch_id - \
        int(closest_step_time_gap / (source_chain_epoch_size * source_chain_block_time))
    if closest_step_epoch_id <= project_first_epoch:
        closest_step_epoch_id = project_first_epoch
        seek_stop_flag = True

    cid_tasks = []
    cid_tasks.append(
        get_project_finalized_cid(
            redis_conn,
            state_contract_obj,
            rpc_helper,
            closest_step_epoch_id,
            project_id,
        ),
    )

    remaining_observations = int((closest_step_timestamp - start_time) / step_seconds)

    count = 0
    head_epoch_id = closest_step_epoch_id
    while not seek_stop_flag and count < remaining_observations:
        tail_epoch_id = head_epoch_id - int(step_seconds / (source_chain_epoch_size * source_chain_block_time))
        if tail_epoch_id <= project_first_epoch:
            tail_epoch_id = project_first_epoch
            seek_stop_flag = True

        cid_tasks.append(
            get_project_finalized_cid(
                redis_conn,
                state_contract_obj,
                rpc_helper,
                tail_epoch_id,
                project_id,
            ),
        )

        head_epoch_id = tail_epoch_id
        count += 1

    all_cids = await asyncio.gather(*cid_tasks)
    project_ids = [project_id for _ in all_cids]

    return await get_submission_data_bulk(
        redis_conn=redis_conn,
        cids=all_cids,
        ipfs_reader=ipfs_reader,
        project_ids=project_ids,
    )
