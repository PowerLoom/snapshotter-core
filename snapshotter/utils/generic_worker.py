import asyncio
import json
import multiprocessing
import resource
import time
from contextlib import asynccontextmanager
from functools import partial
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from typing import Dict
from typing import Set
from typing import Union
from uuid import uuid4

import grpclib
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
from snapshotter.utils.default_logger import default_logger
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


logger = default_logger.bind(module='GenericWorker')


class EIPRequest(EIP712Struct):
    """
    Represents an EIP712 structured request for snapshot submission.
    """
    slotId = Uint()
    deadline = Uint()
    snapshotCid = String()
    epochId = Uint()
    projectId = String()


def web3_storage_retry_state_callback(retry_state: tenacity.RetryCallState):
    """
    Callback function to handle retry attempts for web3 storage upload.

    Args:
        retry_state (tenacity.RetryCallState): The current state of the retry call.

    Returns:
        None
    """
    if retry_state and retry_state.outcome.failed:
        logger.warning(
            f'Encountered web3 storage upload exception: {retry_state.outcome.exception()} | args: {retry_state.args}, kwargs:{retry_state.kwargs}',
        )


def submit_snapshot_retry_callback(retry_state: tenacity.RetryCallState):
    """
    Callback function to handle retry attempts for snapshot submission.

    Args:
        retry_state (tenacity.RetryCallState): The current state of the retry call.

    Returns:
        None
    """
    if retry_state.attempt_number >= 3:
        logger.error(
            'Txn signing worker failed after 3 attempts | Txn payload: {} | Signer: {}', retry_state.kwargs[
                'txn_payload'
            ], retry_state.kwargs['signer_in_use'].address,
        )
    else:
        if retry_state.outcome.failed:
            if 'nonce' in str(retry_state.outcome.exception()):
                # Reassigning the signer object to ensure nonce is reset
                retry_state.kwargs['signer_in_use'] = retry_state.args[0]._signer
                logger.warning(
                    'Tx signing worker attempt number {} result {} failed with nonce exception | Reset nonce and reassigned signer object: {} with nonce {} | Txn payload: {}',
                    retry_state.attempt_number, retry_state.outcome, retry_state.kwargs['signer_in_use'].address,
                    retry_state.kwargs['signer_in_use'].nonce, retry_state.kwargs['txn_payload'],
                )
            else:
                logger.warning(
                    'Tx signing worker attempt number {} result {} failed with exception {} | Txn payload: {}',
                    retry_state.attempt_number, retry_state.outcome, retry_state.outcome.exception(
                    ), retry_state.kwargs['txn_payload'],
                )
        logger.warning(
            'Tx signing worker {} attempt number {} result {} | Txn payload: {}',
            retry_state.kwargs['signer_in_use'].address, retry_state.attempt_number, retry_state.outcome,
            retry_state.kwargs['txn_payload'],
        )


def ipfs_upload_retry_state_callback(retry_state: tenacity.RetryCallState):
    """
    Callback function to handle retry attempts for IPFS uploads.

    Args:
        retry_state (tenacity.RetryCallState): The current state of the retry attempt.

    Returns:
        None
    """
    if retry_state and retry_state.outcome.failed:
        logger.warning(
            f'Encountered ipfs upload exception: {retry_state.outcome.exception()} | args: {retry_state.args}, kwargs:{retry_state.kwargs}',
        )


class GenericAsyncWorker(multiprocessing.Process):
    """
    A generic asynchronous worker class for handling various tasks related to snapshot processing and submission.
    """
    _active_tasks: Set[asyncio.Task]

    def __init__(self, name, **kwargs):
        """
        Initializes a GenericAsyncWorker instance.

        Args:
            name (str): The name of the worker.
            **kwargs: Additional keyword arguments to pass to the superclass constructor.
        """
        self._core_rmq_consumer: asyncio.Task
        self._exchange_name = f'{settings.rabbitmq.setup.callbacks.exchange}:{settings.namespace}'
        self._unique_id = f'{name}-' + keccak(text=str(uuid4())).hex()[:8]
        self._running_callback_tasks: Dict[str, asyncio.Task] = dict()
        super(GenericAsyncWorker, self).__init__(name=name, **kwargs)
        self._protocol_state_contract = None
        self._qos = 5
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
        self._task_timeout = settings.async_task_config.task_timeout
        self._task_cleanup_interval = settings.async_task_config.task_cleanup_interval

        self._last_stream_close_time = 0
        self._stream_lifetime = 30  # Close stream every 30 seconds

    def _signal_handler(self, signum, frame):
        """
        Signal handler function that cancels the core RMQ consumer when a SIGINT, SIGTERM or SIGQUIT signal is received.

        Args:
            signum (int): The signal number.
            frame (frame): The current stack frame at the time the signal was received.
        """
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._core_rmq_consumer.cancel()

    @retry(
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(5),
        retry=tenacity.retry_if_not_exception_type(httpx.HTTPStatusError),
        after=web3_storage_retry_state_callback,
    )
    async def _upload_web3_storage(self, snapshot: bytes):
        """
        Uploads the given snapshot to web3 storage.

        Args:
            snapshot (bytes): The snapshot to upload.

        Returns:
            None

        Raises:
            HTTPError: If the upload fails.
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
        self._logger.debug('Uploaded snapshot to web3 storage: {} | Response: {}', snapshot, resp)

    @retry(
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(5),
        retry=tenacity.retry_if_not_exception_type(IPFSAsyncClientError),
        after=ipfs_upload_retry_state_callback,
        reraise=True,
    )
    async def _upload_to_ipfs(self, snapshot: bytes, _ipfs_writer_client: AsyncIPFSClient):
        """
        Uploads a snapshot to IPFS using the provided AsyncIPFSClient.

        Args:
            snapshot (bytes): The snapshot to upload.
            _ipfs_writer_client (AsyncIPFSClient): The IPFS client to use for uploading.

        Returns:
            str: The CID of the uploaded snapshot.
        """
        snapshot_cid = await _ipfs_writer_client.add_bytes(snapshot)
        return snapshot_cid

    async def generate_signature(self, snapshot_cid, epoch_id, project_id, slot_id=None, private_key=None):
        """
        Generates a signature for the snapshot submission request.

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

        Args:
            task_type (str): The type of task being committed.
            _ipfs_writer_client (AsyncIPFSClient): The IPFS client to use for uploading the snapshot.
            project_id (str): The ID of the project the snapshot belongs to.
            epoch (Union[PowerloomSnapshotProcessMessage, PowerloomSnapshotSubmittedMessage, PowerloomCalculateAggregateMessage]): The epoch the snapshot belongs to.
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
            self._logger.opt(exception=settings.logs.debug_mode).error(
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
                client=self._client,
                message=notification_message,
                redis_conn=self._redis_conn,
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
                self._logger.opt(exception=settings.logs.debug_mode).error(
                    'Exception sending snapshot submitted message to event detector queue: {} | Project: {} | Epoch: {} | Snapshot CID: {}',
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
            await self._create_tracked_task(self._upload_web3_storage(snapshot_bytes))

    async def _rabbitmq_consumer(self, loop):
        """
        Consume messages from a RabbitMQ queue.

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

    async def _init_rpc_helper(self):
        """
        Initializes the RpcHelper objects for the worker and anchor chain, and sets up the protocol state contract.
        """
        self._rpc_helper = RpcHelper(rpc_settings=settings.rpc)
        await self._rpc_helper.init()
        self._anchor_rpc_helper = RpcHelper(rpc_settings=settings.anchor_chain_rpc, source_node=False)
        await self._anchor_rpc_helper.init()
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
        self._chain_id = await self._w3.eth.chain_id
        self._logger.debug('Set anchor chain ID to {}', self._chain_id)
        self._domain_separator = make_domain(
            name='PowerloomProtocolContract', version='0.1', chainId=self._chain_id,
            verifyingContract=self.protocol_state_contract_address,
        )

    async def _init_httpx_client(self):
        """
        Initializes the HTTPX client and transport objects for making HTTP requests.
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

    @asynccontextmanager
    async def open_stream(self):
        """
        Context manager for opening a gRPC stream.

        Yields:
            The opened stream.
        """
        try:
            async with self._grpc_stub.SubmitSnapshot.open() as stream:
                self._stream = stream
                yield self._stream
        finally:
            self._stream = None

    async def _close_stream(self):
        """
        Closes the gRPC stream and logs the response.
        """
        if self._stream:
            try:
                await self._stream.end()
                self._logger.debug('Closed stream')
            except Exception as e:
                self._logger.error(f'Error closing stream: {e}')
            finally:
                self._stream = None

        self._last_stream_close_time = time.time()

    async def _create_tracked_task(self, task):
        """
        Creates and tracks an asynchronous task.

        This method creates a new task from the given coroutine, adds it to the set of active tasks,
        and sets up a callback to remove the task from the set when it's completed.

        Args:
            task (Coroutine): The coroutine to be executed as a task.

        Returns:
            None

        Note:
            This method is used to keep track of all running tasks for potential cleanup or monitoring.
        """
        # Get the current timestamp
        current_time = time.time()

        # Create a new task from the given coroutine
        new_task = asyncio.create_task(task)

        # Add the task to the set of active tasks, along with its creation time
        self._active_tasks.add((current_time, new_task))

        # Set up a callback to remove the task from the set when it's done
        new_task.add_done_callback(lambda _: self._active_tasks.discard((current_time, new_task)))

    async def _send_submission_to_collector(self, snapshot_cid, epoch_id, project_id, slot_id=None, private_key=None):
        """
        Sends a snapshot submission to the collector.

        Args:
            snapshot_cid (str): The CID of the snapshot.
            epoch_id (int): The epoch ID.
            project_id (str): The project ID.
            slot_id (int, optional): The slot ID. Defaults to None.
            private_key (str, optional): The private key to use for signing. Defaults to None.

        Raises:
            Exception: If failed to send the message.
        """
        self._logger.debug(
            'Sending submission to collector...',
        )
        request_, signature, current_block_hash = await self.generate_signature(snapshot_cid, epoch_id, project_id, slot_id, private_key)

        request_msg = Request(
            slotId=request_['slotId'],
            deadline=request_['deadline'],
            snapshotCid=request_['snapshotCid'],
            epochId=request_['epochId'],
            projectId=request_['projectId'],
        )
        self._logger.debug(
            'Snapshot submission creation with request: {}', request_msg,
        )

        msg = SnapshotSubmission(request=request_msg, signature=signature.hex(), header=current_block_hash)
        self._logger.info(
            'Snapshot submission created: {}', msg,
        )
        kwargs_simulation = {'simulation': False}
        if epoch_id == 0:
            kwargs_simulation['simulation'] = True
        # TODO: appropriately use the create tracked task wrapper to handle exceptions as seen below
        try:
            await self.send_message(msg=msg, **kwargs_simulation)
        except Exception as e:
            if 'StreamTerminatedError' in str(e):  # Doing this because we get RetryError here not StreamTerminatedError
                pass  # fail silently as this is intended for the stream to be closed right after sending the message
            else:
                self._logger.error(
                    f'Probable exception in _send_submission_to_collector while sending snapshot to local collector {msg}: {e}',
                )
        else:
            self._logger.info('In _send_submission_to_collector successfully sent snapshot to local collector {msg}')

    @retry(
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(Exception),
    )
    async def send_message(self, msg, simulation=False):
        """
        Sends a message to the collector, either as a simulation or a real submission.

        Args:
            msg (SnapshotSubmission): The message to send.
            simulation (bool, optional): Whether this is a simulation. Defaults to False.

        Raises:
            Exception: If failed to send the message.
        """
        try:
            response = await self._grpc_stub.SubmitSnapshot(msg)
            self._logger.debug(f'Sent message to local collector and received response: {response}')
        except grpclib.GRPCError as e:
            self._logger.error(f'gRPC error occurred while sending snapshot to local collector: {e}')
            raise
        except asyncio.CancelledError:
            self._logger.info('Task to send snapshot to local collector was asyncio cancelled!')
            raise
        except Exception as e:
            self._logger.error(f'Unexpected error occurred while sending snapshot to local collector: {e}')
            raise
        else:
            self._logger.info(f'Successfully submitted snapshot to local collector: {msg}')
        
        return response

    async def _init_grpc(self):
        """
        Initializes the gRPC channel and stub for communication with the collector.
        """
        self._grpc_channel = Channel(
            host='host.docker.internal',
            port=settings.local_collector_port,
            ssl=False,
        )
        self._grpc_stub = SubmissionStub(self._grpc_channel)
        self._stream = None
        self._cancel_task = None

    async def _init_protocol_meta(self):
        """
        Initializes protocol metadata including source chain block time, epoch size, and snapshot submission window.
        """
        # TODO: combine these into a single call
        self._protocol_abi = read_json_file(settings.protocol_state.abi)
        try:
            source_block_time = await self._anchor_rpc_helper.web3_call(
                tasks=[('SOURCE_CHAIN_BLOCK_TIME', [Web3.to_checksum_address(settings.data_market)])],
                contract_addr=self.protocol_state_contract_address,
                abi=self._protocol_abi,
            )
        except Exception as e:
            self._logger.exception(
                'Exception in querying protocol state for source chain block time: {}',
                e,
            )
        else:
            source_block_time = source_block_time[0]
            self._source_chain_block_time = source_block_time / 10 ** 4
            self._logger.debug('Set source chain block time to {}', self._source_chain_block_time)
        try:
            epoch_size = await self._anchor_rpc_helper.web3_call(
                tasks=[('EPOCH_SIZE', [Web3.to_checksum_address(settings.data_market)])],
                contract_addr=self.protocol_state_contract_address,
                abi=self._protocol_abi,
            )
        except Exception as e:
            self._logger.exception(
                'Exception in querying protocol state for epoch size: {}',
                e,
            )
        else:
            self._epoch_size = epoch_size[0]
            self._logger.debug('Set epoch size to {}', self._epoch_size)

    async def init(self):
        """
        Initializes the worker by initializing the Redis pool, HTTPX client, and RPC helper.
        """
        if not self._initialized:
            await self._init_redis_pool()
            await self._init_httpx_client()
            await self._init_rpc_helper()
            await self._init_protocol_meta()
            await self._init_grpc()
            asyncio.create_task(self._cleanup_tasks())

        self._initialized = True

    def run(self) -> None:
        """
        Runs the worker by setting resource limits, registering signal handlers, starting the RabbitMQ consumer, and
        running the event loop until it is stopped.
        """
        self._logger = logger
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )
        for signame in [SIGINT, SIGTERM, SIGQUIT]:
            signal(signame, self._signal_handler)
        ev_loop = asyncio.get_event_loop()
        self._logger.debug(
            f'Starting asynchronous callback worker {self._unique_id}...',
        )
        self._core_rmq_consumer = asyncio.ensure_future(
            self._rabbitmq_consumer(ev_loop),
        )
        try:
            ev_loop.run_forever()
        finally:
            ev_loop.close()

    async def _cleanup_tasks(self):
        """
        Periodically clean up completed or timed-out tasks.
        """
        while True:
            await asyncio.sleep(self._task_cleanup_interval)
            for task_start_time, task in list(self._active_tasks):
                current_time = time.time()
                if task.done():
                    self._active_tasks.discard((task_start_time, task))

                elif current_time - task_start_time > self._task_timeout:
                    self._logger.warning(
                        f'Task {task} timed out. Cancelling..., current_time: {current_time}, start_time: {task_start_time}',
                    )
                    task.cancel()
                    self._active_tasks.discard((task_start_time, task))

