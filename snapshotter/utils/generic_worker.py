import asyncio
import atexit
import json
import multiprocessing
import resource
import time
from functools import partial
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from typing import Dict
from typing import Set
from typing import Union
from uuid import uuid4

import httpx
import sha3
import tenacity
from aio_pika import IncomingMessage
from aio_pika import Message
from aio_pika.pool import Pool
from coincurve import PrivateKey
from eip712_structs import EIP712Struct
from eip712_structs import make_domain
from eip712_structs import String
from eip712_structs import Uint
from eth_utils.crypto import keccak
from eth_utils.encoding import big_endian_to_int
from grpclib.client import Channel
from grpclib.const import Status
from grpclib.exceptions import GRPCError
from grpclib.exceptions import StreamTerminatedError
from httpx import AsyncClient
from httpx import AsyncHTTPTransport
from httpx import Limits
from httpx import Timeout
from ipfs_client.dag import IPFSAsyncClientError
from ipfs_client.main import AsyncIPFSClient
from pydantic import BaseModel
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential
from web3 import Web3

from snapshotter.settings.config import settings
from snapshotter.utils.callback_helpers import get_rabbitmq_channel
from snapshotter.utils.callback_helpers import get_rabbitmq_robust_connection_async
from snapshotter.utils.callback_helpers import send_failure_notifications_async
from snapshotter.utils.data_utils import get_snapshot_submision_window
from snapshotter.utils.default_logger import logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.models.data_models import SnapshotterIssue
from snapshotter.utils.models.data_models import SnapshotterReportState
from snapshotter.utils.models.data_models import SnapshotterStates
from snapshotter.utils.models.data_models import SnapshotterStateUpdate
from snapshotter.utils.models.data_models import UnfinalizedSnapshot
from snapshotter.utils.models.message_models import AggregateBase
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.models.message_models import PowerloomSnapshotSubmittedMessage
from snapshotter.utils.models.proto.snapshot_submission.submission_grpc import SubmissionStub
from snapshotter.utils.models.proto.snapshot_submission.submission_pb2 import Request
from snapshotter.utils.models.proto.snapshot_submission.submission_pb2 import SnapshotSubmission
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import epoch_id_project_to_state_mapping
from snapshotter.utils.redis.redis_keys import submitted_unfinalized_snapshot_cids
from snapshotter.utils.rpc import RpcHelper


class EIPRequest(EIP712Struct):
    """
    Represents an EIP712 structured request for snapshot submission.

    Attributes:
        slotId (Uint): The slot ID for the snapshot.
        deadline (Uint): The deadline for the snapshot submission.
        snapshotCid (String): The CID of the snapshot.
        epochId (Uint): The epoch ID of the snapshot.
        projectId (String): The project ID associated with the snapshot.
    """
    slotId = Uint()
    deadline = Uint()
    snapshotCid = String()
    epochId = Uint()
    projectId = String()


def web3_storage_retry_state_callback(retry_state: tenacity.RetryCallState):
    """
    Callback function to handle retry attempts for web3 storage upload.

    This function logs a warning message when a web3 storage upload fails and a retry is attempted.

    Args:
        retry_state (tenacity.RetryCallState): The current state of the retry call.

    Returns:
        None
    """
    if retry_state and retry_state.outcome.failed:
        logger.warning(
            f'Encountered web3 storage upload exception: {retry_state.outcome.exception()} | '
            f'args: {retry_state.args}, kwargs:{retry_state.kwargs}',
        )


def submit_snapshot_retry_callback(retry_state: tenacity.RetryCallState):
    """
    Callback function to handle retry attempts for snapshot submission.

    This function logs appropriate messages based on the retry attempt status and outcome.

    Args:
        retry_state (tenacity.RetryCallState): The current state of the retry call.

    Returns:
        None
    """
    if retry_state.attempt_number >= 3:
        logger.error(
            'Txn signing worker failed after 3 attempts | Txn payload: {} | Signer: {}',
            retry_state.kwargs['txn_payload'],
            retry_state.kwargs['signer_in_use'].address,
        )
    else:
        if retry_state.outcome.failed:
            if 'nonce' in str(retry_state.outcome.exception()):
                # Reassigning the signer object to ensure nonce is reset
                retry_state.kwargs['signer_in_use'] = retry_state.args[0]._signer
                logger.warning(
                    'Tx signing worker attempt number {} result {} failed with nonce exception | '
                    'Reset nonce and reassigned signer object: {} with nonce {} | Txn payload: {}',
                    retry_state.attempt_number, retry_state.outcome,
                    retry_state.kwargs['signer_in_use'].address,
                    retry_state.kwargs['signer_in_use'].nonce, retry_state.kwargs['txn_payload'],
                )
            else:
                logger.warning(
                    'Tx signing worker attempt number {} result {} failed with exception {} | Txn payload: {}',
                    retry_state.attempt_number, retry_state.outcome,
                    retry_state.outcome.exception(), retry_state.kwargs['txn_payload'],
                )
        logger.warning(
            'Tx signing worker {} attempt number {} result {} | Txn payload: {}',
            retry_state.kwargs['signer_in_use'].address, retry_state.attempt_number,
            retry_state.outcome, retry_state.kwargs['txn_payload'],
        )


def ipfs_upload_retry_state_callback(retry_state: tenacity.RetryCallState):
    """
    Callback function to handle retry attempts for IPFS uploads.

    This function logs a warning message when an IPFS upload fails and a retry is attempted.

    Args:
        retry_state (tenacity.RetryCallState): The current state of the retry attempt.

    Returns:
        None
    """
    if retry_state and retry_state.outcome.failed:
        logger.warning(
            f'Encountered ipfs upload exception: {retry_state.outcome.exception()} | '
            f'args: {retry_state.args}, kwargs:{retry_state.kwargs}',
        )


class StreamPool:
    """
    A pool of gRPC streams for efficient communication with the collector.

    This class manages a pool of gRPC streams, creating new streams as needed and
    removing expired ones.

    Attributes:
        _grpc_stub (SubmissionStub): The gRPC stub for communication.
        pool_size (int): The maximum number of streams in the pool.
        max_stream_age (int): The maximum age of a stream in seconds.
        streams (list): A list of active streams.
        lock (asyncio.Lock): A lock for thread-safe operations on the stream pool.
    """

    def __init__(self, grpc_stub, pool_size=3, max_stream_age=3600):
        """
        Initialize the StreamPool.

        Args:
            grpc_stub (SubmissionStub): The gRPC stub for communication.
            pool_size (int, optional): The maximum number of streams in the pool. Defaults to 3.
            max_stream_age (int, optional): The maximum age of a stream in seconds. Defaults to 3600.
        """
        self._grpc_stub = grpc_stub
        self.pool_size = pool_size
        self.max_stream_age = max_stream_age
        self.streams = []
        self.lock = asyncio.Lock()

    async def get_stream(self):
        """
        Get an available stream from the pool or create a new one if necessary.

        This method removes expired streams, returns an existing stream if available,
        or creates a new stream if needed.

        Returns:
            grpclib.client.Stream: An active gRPC stream.
        """
        async with self.lock:
            current_time = asyncio.get_event_loop().time()

            # Remove expired streams
            self.streams = [s for s in self.streams if (current_time - s['created_at']) < self.max_stream_age]

            if self.streams:
                return self.streams[0]['stream']

            return await self._create_stream()

    async def _create_stream(self):
        """
        Create a new gRPC stream and add it to the pool.

        If the pool size is exceeded after adding the new stream, the oldest stream is removed and closed.

        Returns:
            grpclib.client.Stream: The newly created gRPC stream.
        """
        stream = await self._grpc_stub.SubmitSnapshot.open()
        stream_info = {
            'stream': stream,
            'created_at': asyncio.get_event_loop().time(),
        }
        self.streams.append(stream_info)
        if len(self.streams) > self.pool_size:
            oldest_stream = self.streams.pop(0)
            await oldest_stream['stream'].close()
        return stream

    async def close_all(self):
        """
        Close all streams in the pool and clear the pool.

        This method should be called when shutting down the StreamPool to ensure all streams are properly closed.
        """
        for stream_info in self.streams:
            await stream_info['stream'].close()
        self.streams.clear()


class GenericAsyncWorker(multiprocessing.Process):
    """
    A generic asynchronous worker class for handling various tasks related to snapshot processing and submission.

    This class extends multiprocessing.Process to run as a separate process and handles tasks such as
    snapshot submission, IPFS uploads, and communication with the collector.

    Attributes:
        _active_tasks (Set[asyncio.Task]): A set of active asyncio tasks.
        _core_rmq_consumer (asyncio.Task): The core RabbitMQ consumer task.
        _exchange_name (str): The name of the RabbitMQ exchange.
        _unique_id (str): A unique identifier for this worker instance.
        _running_callback_tasks (Dict[str, asyncio.Task]): A dictionary of running callback tasks.
        _protocol_state_contract (Contract): The protocol state contract.
        _qos (int): The quality of service for RabbitMQ consumption.
        _signer_index (int): The index of the signer to use.
        _rate_limiting_lua_scripts (Any): Rate limiting Lua scripts.
        protocol_state_contract_address (str): The address of the protocol state contract.
        _commit_payload_exchange (str): The name of the commit payload exchange.
        _event_detector_exchange (str): The name of the event detector exchange.
        _event_detector_routing_key_prefix (str): The routing key prefix for the event detector.
        _commit_payload_routing_key (str): The routing key for commit payloads.
        _keccak_hash (Callable): A function to compute Keccak hash.
        _private_key (str): The private key for signing transactions.
        _identity_private_key (PrivateKey): The private key object for signing.
        _initialized (bool): Whether the worker has been initialized.
        _task_timeout (int): The timeout for tasks in seconds.
        _task_cleanup_interval (int): The interval for cleaning up tasks in seconds.
        stream_manager_task (asyncio.Task): The stream manager task.
    """

    _active_tasks: Set[asyncio.Task]

    def __init__(self, name, signer_idx, **kwargs):
        """
        Initialize a GenericAsyncWorker instance.

        Args:
            name (str): The name of the worker.
            signer_idx (int): The index of the signer to use from the list in settings.
            **kwargs: Additional keyword arguments to pass to the superclass constructor.
        """
        self._core_rmq_consumer: asyncio.Task
        self._exchange_name = f'{settings.rabbitmq.setup.callbacks.exchange}:{settings.namespace}'
        self._unique_id = f'{name}-' + keccak(text=str(uuid4())).hex()[:8]
        self._running_callback_tasks: Dict[str, asyncio.Task] = dict()
        super(GenericAsyncWorker, self).__init__(name=name, **kwargs)
        self._protocol_state_contract = None
        self._qos = 1
        self._signer_index = signer_idx
        self._rate_limiting_lua_scripts = None

        self.protocol_state_contract_address = Web3.to_checksum_address(settings.protocol_state.address)
        self._commit_payload_exchange = (
            f'{settings.rabbitmq.setup.commit_payload.exchange}:{settings.namespace}'
        )
        self._event_detector_exchange = f'{settings.rabbitmq.setup.event_detector.exchange}:{settings.namespace}'
        self._event_detector_routing_key_prefix = f'powerloom-event-detector:{settings.namespace}:{settings.instance_id}.'
        self._commit_payload_routing_key = (
            f'powerloom-backend-commit-payload:{settings.namespace}:{settings.instance_id}.Data'
        )
        self._keccak_hash = lambda x: sha3.keccak_256(x).digest()
        self._private_key = settings.signer_private_key
        if self._private_key.startswith('0x'):
            self._private_key = self._private_key[2:]
        self._identity_private_key = PrivateKey.from_hex(settings.signer_private_key)
        self._initialized = False
        # Task tracking
        self._active_tasks: Set[asyncio.Task] = set()
        self._task_timeout = 300  # 5 minutes
        self._task_cleanup_interval = 60  # 1 minute
        self.stream_manager_task = None

    def _signal_handler(self, signum, frame):
        """
        Signal handler function that initiates the cleanup process when a SIGINT, SIGTERM or SIGQUIT signal is received.
        """
        self._logger.info(f'Received signal {signum}. Initiating shutdown...')
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            asyncio.create_task(self._shutdown())

    async def _shutdown(self):
        """
        Performs a graceful shutdown of the worker.
        """
        self._logger.info('Shutting down worker...')
        if hasattr(self, '_core_rmq_consumer'):
            self._core_rmq_consumer.cancel()
        await self.cleanup()
        asyncio.get_event_loop().stop()

    @retry(
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(5),
        retry=tenacity.retry_if_not_exception_type(httpx.HTTPStatusError),
        after=web3_storage_retry_state_callback,
    )
    async def _upload_web3_storage(self, snapshot: bytes):
        """
        Uploads the given snapshot to web3 storage.

        This method attempts to upload the snapshot to web3 storage, with retries on failure.

        Args:
            snapshot (bytes): The snapshot to upload.

        Returns:
            None

        Raises:
            HTTPError: If the upload fails after all retry attempts.
        """
        web3_storage_settings = settings.web3storage
        # If no API token is provided, skip
        if not web3_storage_settings.api_token:
            return
        files = {'file': snapshot}
        r = await self._web3_storage_upload_client.post(
            url=f'{web3_storage_settings.url}{web3_storage_settings.upload_url_suffix}',
            files=files,
        )
        r.raise_for_status()
        resp = r.json()
        self._logger.info('Uploaded snapshot to web3 storage: {} | Response: {}', snapshot, resp)

    @retry(
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(5),
        retry=tenacity.retry_if_not_exception_type(IPFSAsyncClientError),
        after=ipfs_upload_retry_state_callback,
    )
    async def _upload_to_ipfs(self, snapshot: bytes, _ipfs_writer_client: AsyncIPFSClient):
        """
        Uploads a snapshot to IPFS using the provided AsyncIPFSClient.

        This method attempts to upload the snapshot to IPFS, with retries on failure.

        Args:
            snapshot (bytes): The snapshot to upload.
            _ipfs_writer_client (AsyncIPFSClient): The IPFS client to use for uploading.

        Returns:
            str: The CID of the uploaded snapshot.

        Raises:
            IPFSAsyncClientError: If the upload fails after all retry attempts.
        """
        snapshot_cid = await _ipfs_writer_client.add_bytes(snapshot)
        return snapshot_cid

    async def generate_signature(self, snapshot_cid, epoch_id, project_id, slot_id=None, private_key=None):
        """
        Generates a signature for the snapshot submission request.

        This method creates an EIP712 structured request and signs it with the provided or default private key.

        Args:
            snapshot_cid (str): The CID of the snapshot.
            epoch_id (int): The epoch ID.
            project_id (str): The project ID.
            slot_id (int, optional): The slot ID. Defaults to None.
            private_key (str, optional): The private key to use for signing. Defaults to None.

        Returns:
            tuple: A tuple containing the request, signature, and current block hash.
        """
        current_block = await self._anchor_rpc_helper.eth_get_block()
        current_block_number = int(current_block['number'], 16)
        current_block_hash = current_block['hash']
        deadline = current_block_number + settings.protocol_state.deadline_buffer
        request_slot_id = settings.slot_id if not slot_id else slot_id
        request = EIPRequest(
            slotId=request_slot_id,
            deadline=deadline,
            snapshotCid=snapshot_cid,
            epochId=epoch_id,
            projectId=project_id,
        )

        signable_bytes = request.signable_bytes(self._domain_separator)
        if not private_key:  # self signing
            signature = self._identity_private_key.sign_recoverable(signable_bytes, hasher=self._keccak_hash)
        else:
            if private_key.startswith('0x'):
                private_key = private_key[2:]
            signer_private_key = PrivateKey.from_hex(private_key)
            signature = signer_private_key.sign_recoverable(signable_bytes, hasher=self._keccak_hash)
        v = signature[64] + 27
        r = big_endian_to_int(signature[0:32])
        s = big_endian_to_int(signature[32:64])

        final_sig = r.to_bytes(32, 'big') + s.to_bytes(32, 'big') + v.to_bytes(1, 'big')
        request_ = {
            'slotId': request_slot_id, 'deadline': deadline,
            'snapshotCid': snapshot_cid, 'epochId': epoch_id, 'projectId': project_id,
        }
        return request_, final_sig, current_block_hash

    async def _commit_payload(
            self,
            task_type: str,
            _ipfs_writer_client: AsyncIPFSClient,
            project_id: str,
            epoch: Union[
                PowerloomSnapshotProcessMessage,
                PowerloomSnapshotSubmittedMessage,
                PowerloomCalculateAggregateMessage,
            ],
            snapshot: Union[BaseModel, AggregateBase],
            storage_flag: bool,
    ):
        """
        Commits the given snapshot to IPFS and web3 storage (if enabled), and sends messages to the event detector and relayer
        dispatch queues.

        This method handles the entire process of committing a snapshot, including uploading to IPFS,
        sending notifications, and updating Redis state.

        Args:
            task_type (str): The type of task being committed.
            _ipfs_writer_client (AsyncIPFSClient): The IPFS client to use for uploading the snapshot.
            project_id (str): The ID of the project the snapshot belongs to.
            epoch (Union[PowerloomSnapshotProcessMessage, PowerloomSnapshotSubmittedMessage, PowerloomCalculateAggregateMessage]):
                The epoch the snapshot belongs to.
            snapshot (Union[BaseModel, AggregateBase]): The snapshot to commit.
            storage_flag (bool): Whether to upload the snapshot to web3 storage.

        Returns:
            None
        """
        # Payload commit sequence begins
        # Upload to IPFS
        snapshot_json = json.dumps(snapshot.dict(by_alias=True), sort_keys=True, separators=(',', ':'))
        snapshot_bytes = snapshot_json.encode('utf-8')
        try:
            snapshot_cid = await self._upload_to_ipfs(snapshot_bytes, _ipfs_writer_client)
        except Exception as e:
            self._logger.opt(exception=True).error(
                'Exception uploading snapshot to IPFS for epoch {}: {}, Error: {},'
                'sending failure notifications', epoch, snapshot, e,
            )
            notification_message = SnapshotterIssue(
                instanceID=settings.instance_id,
                issueType=SnapshotterReportState.MISSED_SNAPSHOT.value,
                projectID=project_id,
                epochId=str(epoch.epochId),
                timeOfReporting=str(time.time()),
                extra=json.dumps({'issueDetails': f'Error : {e}'}),
            )
            await send_failure_notifications_async(
                client=self._client, message=notification_message,
            )
        else:
            # Add to zset of unfinalized snapshot CIDs
            unfinalized_entry = UnfinalizedSnapshot(
                snapshotCid=snapshot_cid,
                snapshot=snapshot.dict(by_alias=True),
            )
            await self._redis_conn.zadd(
                name=submitted_unfinalized_snapshot_cids(project_id),
                mapping={unfinalized_entry.json(sort_keys=True): epoch.epochId},
            )
            # Publish snapshot submitted event to event detector queue
            snapshot_submitted_message = PowerloomSnapshotSubmittedMessage(
                snapshotCid=snapshot_cid,
                epochId=epoch.epochId,
                projectId=project_id,
                timestamp=int(time.time()),
            )
            try:
                async with self._rmq_connection_pool.acquire() as connection:
                    async with self._rmq_channel_pool.acquire() as channel:
                        # Prepare a message to send
                        commit_payload_exchange = await channel.get_exchange(
                            name=self._event_detector_exchange,
                        )
                        message_data = snapshot_submitted_message.json().encode()

                        # Prepare a message to send
                        message = Message(message_data)

                        await commit_payload_exchange.publish(
                            message=message,
                            routing_key=self._event_detector_routing_key_prefix + 'SnapshotSubmitted',
                        )

                        self._logger.debug(
                            'Sent snapshot submitted message to event detector queue | '
                            'Project: {} | Epoch: {} | Snapshot CID: {}',
                            project_id, epoch.epochId, snapshot_cid,
                        )

            except Exception as e:
                self._logger.opt(exception=True).error(
                    'Exception sending snapshot submitted message to event detector queue: {} | '
                    'Project: {} | Epoch: {} | Snapshot CID: {}',
                    e, project_id, epoch.epochId, snapshot_cid,
                )

            try:
                # Remove old unfinalized snapshots
                await self._redis_conn.zremrangebyscore(
                    name=submitted_unfinalized_snapshot_cids(project_id),
                    min='-inf',
                    max=epoch.epochId - 32,
                )
            except:
                pass

            try:
                await self._send_submission_to_collector(snapshot_cid, epoch.epochId, project_id)
            except Exception as e:
                self._logger.error(
                    'Exception submitting snapshot to collector for epoch {}: {}, Error: {},'
                    'sending failure notifications', epoch, snapshot, e,
                )
                await self._redis_conn.hset(
                    name=epoch_id_project_to_state_mapping(
                        epoch.epochId, SnapshotterStates.SNAPSHOT_SUBMIT_COLLECTOR.value,
                    ),
                    mapping={
                        project_id: SnapshotterStateUpdate(
                            status='failed', error=str(e), timestamp=int(time.time()),
                        ).json(),
                    },
                )
            else:
                await self._redis_conn.hset(
                    name=epoch_id_project_to_state_mapping(
                        epoch.epochId, SnapshotterStates.SNAPSHOT_SUBMIT_COLLECTOR.value,
                    ),
                    mapping={
                        project_id: SnapshotterStateUpdate(
                            status='success', timestamp=int(time.time()),
                        ).json(),
                    },
                )

        # Upload to web3 storage
        if storage_flag:
            asyncio.ensure_future(self._upload_web3_storage(snapshot_bytes))

    async def _rabbitmq_consumer(self, loop):
        """
        Consume messages from a RabbitMQ queue.

        This method sets up the RabbitMQ connection, channel, and starts consuming messages from the specified queue.

        Args:
            loop (asyncio.AbstractEventLoop): The event loop to use for the consumer.

        Returns:
            None
        """
        self._rmq_connection_pool = Pool(get_rabbitmq_robust_connection_async, max_size=5, loop=loop)
        self._rmq_channel_pool = Pool(
            partial(get_rabbitmq_channel, self._rmq_connection_pool), max_size=20,
            loop=loop,
        )
        async with self._rmq_channel_pool.acquire() as channel:
            await channel.set_qos(self._qos)
            exchange = await channel.get_exchange(
                name=self._exchange_name,
            )
            q_obj = await channel.get_queue(
                name=self._q,
                ensure=False,
            )
            self._logger.debug(
                f'Consuming queue {self._q} with routing key {self._rmq_routing}...',
            )
            await q_obj.bind(exchange, routing_key=self._rmq_routing)
            await q_obj.consume(self._on_rabbitmq_message)

    async def _on_rabbitmq_message(self, message: IncomingMessage):
        """
        Callback function that is called when a message is received from RabbitMQ.

        This method should be overridden in subclasses to implement specific message handling logic.

        Args:
            message (IncomingMessage): The incoming message from RabbitMQ.
        """
        pass

    async def _init_redis_pool(self):
        """
        Initializes the Redis connection pool and sets the `_redis_conn` attribute to the created connection pool.
        """
        self._aioredis_pool = RedisPoolCache()
        await self._aioredis_pool.populate()
        self._redis_conn = self._aioredis_pool._aioredis_pool

    @retry(
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(Exception),
    )
    async def _get_chain_id(self):
        """
        Gets the chain ID for the worker.

        This method attempts to retrieve the chain ID, with retries on failure.

        Returns:
            int: The chain ID.
        """
        self._chain_id = await self._w3.eth.chain_id
        self._logger.debug('Set anchor chain ID to {}', self._chain_id)
        return self._chain_id

    async def _init_rpc_helper(self):
        """
        Initializes the RpcHelper objects for the worker and anchor chain, and sets up the protocol state contract.

        This method initializes RPC helpers for both the main chain and the anchor chain, sets up the protocol state contract,
        and initializes the Web3 instance and domain separator.
        """
        self._rpc_helper = RpcHelper(rpc_settings=settings.rpc)
        await self._rpc_helper.init(redis_conn=self._redis_conn)
        self._anchor_rpc_helper = RpcHelper(rpc_settings=settings.anchor_chain_rpc)
        await self._anchor_rpc_helper.init(redis_conn=self._redis_conn)
        await self._anchor_rpc_helper._load_async_web3_providers()
        self._protocol_state_contract = self._anchor_rpc_helper.get_current_node()['web3_client'].eth.contract(
            address=Web3.to_checksum_address(
                self.protocol_state_contract_address,
            ),
            abi=read_json_file(
                settings.protocol_state.abi,
                self._logger,
            ),
        )
        self._w3 = self._anchor_rpc_helper._nodes[0]['web3_client_async']

        self._chain_id = await self._get_chain_id()
        self._logger.debug('Set anchor chain ID to {}', self._chain_id)
        self._domain_separator = make_domain(
            name='PowerloomProtocolContract', version='0.1', chainId=self._chain_id,
            verifyingContract=self.protocol_state_contract_address,
        )

    async def _init_httpx_client(self):
        """
        Initializes the HTTPX client and transport objects for making HTTP requests.

        This method sets up two HTTPX clients: one for general use and another specifically for web3 storage uploads.
        """
        self._async_transport = AsyncHTTPTransport(
            limits=Limits(
                max_connections=200,
                max_keepalive_connections=50,
                keepalive_expiry=None,
            ),
        )
        self._client = AsyncClient(
            timeout=Timeout(timeout=5.0),
            follow_redirects=False,
            transport=self._async_transport,
        )
        self._web3_storage_upload_transport = AsyncHTTPTransport(
            limits=Limits(
                max_connections=200,
                max_keepalive_connections=settings.web3storage.max_idle_conns,
                keepalive_expiry=settings.web3storage.idle_conn_timeout,
            ),
        )
        self._web3_storage_upload_client = AsyncClient(
            timeout=Timeout(timeout=settings.web3storage.timeout),
            follow_redirects=False,
            transport=self._web3_storage_upload_transport,
            headers={'Authorization': 'Bearer ' + settings.web3storage.api_token},
        )

    async def _init_grpc(self):
        """
        Initializes the gRPC channel and stub for communication with the collector.

        This method sets up a gRPC channel to communicate with the local collector service,
        creates a submission stub, and initializes a stream pool for efficient message handling.
        """
        # Create a gRPC channel to the local collector service
        self._grpc_channel = Channel(
            host='host.docker.internal',
            port=settings.local_collector_port,
            ssl=False,
        )
        # Initialize the submission stub for making gRPC calls
        self._grpc_stub = SubmissionStub(self._grpc_channel)
        # Create a pool of streams for efficient message handling
        self.stream_pool = StreamPool(self._grpc_stub)

    async def send_message(self, msg, simulation=False):
        """
        Sends a message to the collector, either as a simulation or a real submission.

        This method attempts to send a message to the collector service, with retry logic
        in case of failures. It handles both simulation and real submission scenarios.

        Args:
            msg (SnapshotSubmission): The message to send.
            simulation (bool, optional): Whether this is a simulation. Defaults to False.

        Raises:
            Exception: If failed to send the message after retries.
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if simulation:
                    # Handle simulation submission
                    async with self._grpc_stub.SubmitSnapshotSimulation.open() as stream:
                        await stream.send_message(msg)
                        self._logger.debug(f'Sent simulation message: {msg}')

                        response = await stream.recv_message()
                        await stream.end()

                        if response and 'Success' in response.message:
                            self._logger.info('✅ Simulation snapshot submitted successfully: {}!', msg)
                            return {'status_code': 200}
                        else:
                            raise Exception(f'Failed to send simulation snapshot, got response: {response}')
                else:
                    # Handle real submission
                    stream = await self.stream_pool.get_stream()
                    async with stream:
                        await stream.send_message(msg)
                        self._logger.debug(f'Sent message: {msg}')
                        return {'status_code': 200}
            except StreamTerminatedError as e:
                self._logger.warning(f'Stream terminated while sending message. Retrying... Error: {e}')
                if attempt == max_retries - 1:
                    raise Exception(f'Failed to send message after {max_retries} attempts: {e}')
            except GRPCError as e:
                if e.status == Status.UNAVAILABLE:
                    self._logger.warning(f'gRPC service unavailable. Retrying... Error: {e}')
                    await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
                else:
                    raise Exception(f'gRPC error: {e}')
            except Exception as e:
                self._logger.error(f'Failed to send message: {e}')
                raise Exception(f'Failed to send message: {e}')

    async def cleanup(self):
        """
        Cleans up resources by closing all streams in the pool and the gRPC channel.
        """
        self._logger.info('Cleaning up resources...')
        if hasattr(self, 'stream_pool'):
            await self.stream_pool.close_all()
        if hasattr(self, '_grpc_channel'):
            await self._grpc_channel.close()
        if hasattr(self, '_client'):
            await self._client.aclose()
        if hasattr(self, '_web3_storage_upload_client'):
            await self._web3_storage_upload_client.aclose()
        self._logger.info('Cleanup completed.')

    def run(self) -> None:
        """
        Runs the worker by setting up the environment and starting the main event loop.
        """
        self._logger = logger.bind(module=self.name)

        # Set resource limits
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )

        # Register signal handlers
        for signame in [SIGINT, SIGTERM, SIGQUIT]:
            signal(signame, self._signal_handler)

        # Register cleanup function to be called at exit
        atexit.register(lambda: asyncio.get_event_loop().run_until_complete(self.cleanup()))

        # Set up and run the event loop
        ev_loop = asyncio.get_event_loop()
        self._logger.debug(f'Starting asynchronous callback worker {self._unique_id}...')
        self._core_rmq_consumer = asyncio.ensure_future(self._rabbitmq_consumer(ev_loop))
        try:
            ev_loop.run_forever()
        finally:
            ev_loop.run_until_complete(self.cleanup())
            ev_loop.close()

    async def _cleanup_tasks(self):
        """
        Periodically clean up completed or timed-out tasks.

        This method runs in an infinite loop, periodically checking all active tasks
        and removing those that are completed or have timed out.
        """
        while True:
            await asyncio.sleep(self._task_cleanup_interval)
            current_time = time.time()
            for task in list(self._active_tasks):
                if task.done():
                    self._active_tasks.discard(task)
                elif current_time - task.get_coro().cr_frame.f_locals.get('start_time', 0) > self._task_timeout:
                    self._logger.warning(f'Task {task} timed out. Cancelling...')
                    task.cancel()
                    self._active_tasks.discard(task)

    async def _send_submission_to_collector(self, snapshot_cid, epoch_id, project_id, slot_id=None, private_key=None):
        """
        Sends a snapshot submission to the collector.

        This method prepares a snapshot submission message, signs it, and sends it to the collector.
        It handles both simulation (epoch_id = 0) and real submissions.

        Args:
            snapshot_cid (str): The CID of the snapshot.
            epoch_id (int): The epoch ID.
            project_id (str): The project ID.
            slot_id (int, optional): The slot ID. Defaults to None.
            private_key (str, optional): The private key to use for signing. Defaults to None.

        Raises:
            Exception: If failed to send the message.
        """
        self._logger.debug('Sending submission to collector...')

        # Generate signature for the submission
        request_, signature, current_block_hash = await self.generate_signature(snapshot_cid, epoch_id, project_id, slot_id, private_key)

        # Prepare the request message
        request_msg = Request(
            slotId=request_['slotId'],
            deadline=request_['deadline'],
            snapshotCid=request_['snapshotCid'],
            epochId=request_['epochId'],
            projectId=request_['projectId'],
        )
        self._logger.debug('Snapshot submission creation with request: {}', request_msg)

        # Create the final submission message
        msg = SnapshotSubmission(request=request_msg, signature=signature.hex(), header=current_block_hash)
        self._logger.debug('Snapshot submission created: {}', msg)

        try:
            # Send the message, handling simulation case separately
            if epoch_id == 0:
                await self.send_message(msg, simulation=True)
            else:
                await self.send_message(msg)
        except Exception as e:
            self._logger.opt(exception=True).error(f'Failed to send message: {e}')
            raise Exception(f'Failed to send message: {e}')

    async def _init_protocol_meta(self):
        """
        Initializes protocol metadata including source chain block time, epoch size, and snapshot submission window.

        This method queries the blockchain for various protocol-specific parameters and sets them as instance variables.
        It handles exceptions for each query separately to ensure partial initialization in case of failures.
        """
        # TODO: combine these into a single call for efficiency
        self._protocol_abi = read_json_file(settings.protocol_state.abi)

        # Query and set source chain block time
        try:
            source_block_time = await self._anchor_rpc_helper.web3_call(
                tasks=[('SOURCE_CHAIN_BLOCK_TIME', [Web3.to_checksum_address(settings.data_market)])],
                contract_addr=self.protocol_state_contract_address,
                abi=self._protocol_abi,
            )
            source_block_time = source_block_time[0]
            self._source_chain_block_time = source_block_time / 10 ** 4
            self._logger.debug('Set source chain block time to {}', self._source_chain_block_time)
        except Exception as e:
            self._logger.exception('Exception in querying protocol state for source chain block time: {}', e)

        # Query and set epoch size
        try:
            epoch_size = await self._anchor_rpc_helper.web3_call(
                tasks=[('EPOCH_SIZE', [Web3.to_checksum_address(settings.data_market)])],
                contract_addr=self.protocol_state_contract_address,
                abi=self._protocol_abi,
            )
            self._epoch_size = epoch_size[0]
            self._logger.debug('Set epoch size to {}', self._epoch_size)
        except Exception as e:
            self._logger.exception('Exception in querying protocol state for epoch size: {}', e)

        # Query and set submission window
        try:
            submission_window = await get_snapshot_submision_window(
                redis_conn=self._redis_conn,
                rpc_helper=self._anchor_rpc_helper,
                state_contract_obj=self._protocol_state_contract,
            )
            self._submission_window = submission_window
            self._logger.debug('Set snapshot submission window to {}', self._submission_window)
        except Exception as e:
            self._logger.exception('Exception in querying protocol state for snapshot submission window: {}', e)

    async def init(self):
        """
        Initializes the worker by setting up all necessary components.

        This method initializes the Redis pool, HTTPX client, RPC helper, protocol metadata,
        gRPC connection, and starts the task cleanup process. It ensures that initialization
        happens only once.
        """
        if not self._initialized:
            await self._init_redis_pool()
            await self._init_httpx_client()
            await self._init_rpc_helper()
            await self._init_protocol_meta()
            await self._init_grpc()
            asyncio.create_task(self._cleanup_tasks())

        self._initialized = True
