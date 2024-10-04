import asyncio
import importlib
import multiprocessing
import queue
import resource
import sys
import time
from collections import defaultdict
from functools import lru_cache
from functools import partial
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from typing import Awaitable
from typing import Dict
from typing import List
from typing import Set
from uuid import uuid4

import uvloop
from aio_pika import IncomingMessage
from aio_pika import Message
from aio_pika.pool import Pool
from eth_utils.address import to_checksum_address
from eth_utils.crypto import keccak
from httpx import AsyncClient
from httpx import AsyncHTTPTransport
from httpx import Limits
from httpx import Timeout
from pydantic import ValidationError
from redis import asyncio as aioredis
from web3 import Web3

from snapshotter.settings.config import aggregator_config
from snapshotter.settings.config import preloaders
from snapshotter.settings.config import projects_config
from snapshotter.settings.config import settings
from snapshotter.utils.callback_helpers import get_rabbitmq_channel
from snapshotter.utils.callback_helpers import get_rabbitmq_robust_connection_async
from snapshotter.utils.data_utils import get_source_chain_epoch_size
from snapshotter.utils.data_utils import get_source_chain_id
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.models.data_models import SnapshotterStates
from snapshotter.utils.models.data_models import SnapshotterStateUpdate
from snapshotter.utils.models.data_models import SnapshottersUpdatedEvent
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.models.message_models import PowerloomProjectsUpdatedMessage
from snapshotter.utils.models.message_models import PowerloomSnapshotFinalizedMessage
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.models.message_models import PowerloomSnapshotSubmittedMessage
from snapshotter.utils.models.message_models import ProcessHubCommand
from snapshotter.utils.models.settings_model import AggregateOn
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import active_status_key
from snapshotter.utils.redis.redis_keys import epoch_id_epoch_released_key
from snapshotter.utils.redis.redis_keys import epoch_id_project_to_state_mapping
from snapshotter.utils.redis.redis_keys import process_hub_core_start_timestamp
from snapshotter.utils.redis.redis_keys import project_finalized_data_zset
from snapshotter.utils.redis.redis_keys import project_last_finalized_epoch_key
from snapshotter.utils.rpc import RpcHelper


class ProcessorDistributor(multiprocessing.Process):
    """
    A class responsible for distributing processing tasks and managing the snapshot lifecycle.

    This class handles epoch releases, project updates, snapshot submissions, and aggregations.
    It interacts with RabbitMQ for message passing and Redis for state management.
    """

    _aioredis_pool: RedisPoolCache
    _redis_conn: aioredis.Redis
    _rpc_helper: RpcHelper
    _anchor_rpc_helper: RpcHelper
    _async_transport: AsyncHTTPTransport
    _client: AsyncClient
    _snapshot_build_awaited_project_ids: Dict[int, Set[str]]  # epoch_id: project_ids
    _slot_id_to_snapshotters: Dict[int, Dict[str, str]]  # slot_id: {snapshotters}
    _slot_id_to_timeslot: Dict[int, int]  # slot_id: timeslot
    _registered_slots: List[int]
    _last_synced_slot_info: int
    _source_chain_epoch_size: int
    _source_chain_id: int

    def __init__(self, name, **kwargs):
        """
        Initialize the ProcessorDistributor object.

        Args:
            name (str): The name of the ProcessorDistributor.
            **kwargs: Additional keyword arguments.

        Attributes:
            _unique_id (str): The unique ID of the ProcessorDistributor.
            _q (queue.Queue): The queue used for processing tasks.
            _rabbitmq_interactor: The RabbitMQ interactor object.
            _shutdown_initiated (bool): Flag indicating if shutdown has been initiated.
            _rpc_helper: The RPC helper object.
            _source_chain_id: The source chain ID.
            _projects_list: The list of projects.
            _consume_exchange_name (str): The name of the exchange for consuming events.
            _consume_queue_name (str): The name of the queue for consuming events.
            _initialized (bool): Flag indicating if the ProcessorDistributor has been initialized.
            _consume_queue_routing_key (str): The routing key for consuming events.
            _callback_exchange_name (str): The name of the exchange for callbacks.
            _payload_commit_exchange_name (str): The name of the exchange for payload commits.
            _payload_commit_routing_key (str): The routing key for payload commits.
            _upcoming_project_changes (defaultdict): Dictionary of upcoming project changes.
            _preload_completion_conditions (defaultdict): Dictionary of preload completion conditions.
            _newly_added_projects (set): Set of newly added projects.
            _shutdown_initiated (bool): Flag indicating if shutdown has been initiated.
            _all_preload_tasks (set): Set of all preload tasks.
            _project_type_config_mapping (dict): Dictionary mapping project types to their configurations.
            _last_epoch_processing_health_check (int): Timestamp of the last epoch processing health check.
            _preloader_compute_mapping (dict): Dictionary mapping preloader tasks to compute resources.
        """
        super(ProcessorDistributor, self).__init__(name=name, **kwargs)
        self._unique_id = f'{name}-' + keccak(text=str(uuid4())).hex()[:8]
        self._logger = default_logger.bind(
            module=f'Callbacks|ProcessDistributor:{settings.namespace}-{settings.instance_id}',
        )
        self._q = queue.Queue()
        self._rabbitmq_interactor = None
        self._shutdown_initiated = False
        self._consume_exchange_name = f'{settings.rabbitmq.setup.event_detector.exchange}:{settings.namespace}'
        self._consume_queue_name = (
            f'powerloom-event-detector:{settings.namespace}:{settings.instance_id}'
        )

        self._initialized = False
        self._consume_queue_routing_key = f'powerloom-event-detector:{settings.namespace}:{settings.instance_id}.*'
        self._callback_exchange_name = (
            f'{settings.rabbitmq.setup.callbacks.exchange}:{settings.namespace}'
        )
        self._payload_commit_exchange_name = (
            f'{settings.rabbitmq.setup.commit_payload.exchange}:{settings.namespace}'
        )
        self._payload_commit_routing_key = (
            f'powerloom-backend-commit-payload:{settings.namespace}:{settings.instance_id}.Finalized'
        )

        self._upcoming_project_changes = defaultdict(list)
        self._preload_completion_conditions: Dict[int, Dict] = defaultdict(
            dict,
        )  # epoch ID to preloading complete event

        self._newly_added_projects = set()
        self._shutdown_initiated = False
        self._all_preload_tasks = set()
        self._project_type_config_mapping = dict()
        for project_config in projects_config:
            self._project_type_config_mapping[project_config.project_type] = project_config
            for preload_task in project_config.preload_tasks:
                self._all_preload_tasks.add(preload_task)

        self._aggregator_config_mapping = dict()
        for agg_config in aggregator_config:
            self._aggregator_config_mapping[agg_config.project_type] = agg_config

        self._logger.debug('All preload tasks by string ID during init: {}', self._all_preload_tasks)
        self._last_epoch_processing_health_check = 0
        self._preloader_compute_mapping = dict()
        self._snapshot_build_awaited_project_ids = dict()
        # Task tracking
        self._active_tasks: Set[asyncio.Task] = set()
        self._task_timeout = settings.async_task_config.task_timeout
        self._task_cleanup_interval = settings.async_task_config.task_cleanup_interval

    def _signal_handler(self, signum, frame):
        """
        Signal handler method that cancels the core RMQ consumer when a SIGINT, SIGTERM, or SIGQUIT signal is received.

        Args:
            signum (int): The signal number.
            frame (frame): The current stack frame at the time the signal was received.
        """
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._core_rmq_consumer.cancel()

    async def _init_redis_pool(self):
        """
        Initializes the Redis connection pool and populates it with connections.
        """
        self._aioredis_pool = RedisPoolCache()
        await self._aioredis_pool.populate()
        self._redis_conn = self._aioredis_pool._aioredis_pool

    async def _init_rpc_helper(self):
        """
        Initializes the RpcHelper instance if it is not already initialized.
        """
        self._rpc_helper = RpcHelper()
        await self._rpc_helper.init()
        self._anchor_rpc_helper = RpcHelper(rpc_settings=settings.anchor_chain_rpc, source_node=False)
        await self._anchor_rpc_helper.init()

    async def _init_rabbitmq_connection(self):
        """
        Initializes the RabbitMQ connection pool and channel pool.

        The RabbitMQ connection pool is used to manage a pool of connections to the RabbitMQ server,
        while the channel pool is used to manage a pool of channels for each connection.

        Returns:
            None
        """
        self._rmq_connection_pool = Pool(
            get_rabbitmq_robust_connection_async,
            max_size=20, loop=asyncio.get_event_loop(),
        )
        self._rmq_channel_pool = Pool(
            partial(get_rabbitmq_channel, self._rmq_connection_pool), max_size=100,
            loop=asyncio.get_event_loop(),
        )

    async def _init_httpx_client(self):
        """
        Initializes the HTTPX client to send reports to reporting service with the specified settings.
        """
        self._async_transport = AsyncHTTPTransport(
            limits=Limits(
                max_connections=10,
                max_keepalive_connections=5,
                keepalive_expiry=None,
            ),
        )
        self._client = AsyncClient(
            base_url=settings.reporting.service_url,
            timeout=Timeout(timeout=5.0),
            follow_redirects=False,
            transport=self._async_transport,
        )

    async def _send_proc_hub_respawn(self):
        """
        Sends a respawn command to the process hub.

        This method creates a ProcessHubCommand object with the command 'respawn',
        acquires a channel from the channel pool, gets the exchange, and publishes
        the command message to the exchange.

        Args:
            None

        Returns:
            None
        """
        proc_hub_cmd = ProcessHubCommand(
            command='respawn',
        )
        async with self._rmq_channel_pool.acquire() as channel:
            await channel.set_qos(10)
            exchange = await channel.get_exchange(
                name=f'{settings.rabbitmq.setup.core.exchange}:{settings.namespace}',
            )
            await exchange.publish(
                routing_key=f'processhub-commands:{settings.namespace}:{settings.instance_id}',
                message=Message(proc_hub_cmd.json().encode('utf-8')),
            )

    async def _init_preloader_compute_mapping(self):
        """
        Initializes the preloader compute mapping by importing the preloader module and class and
        adding it to the mapping dictionary.
        """
        if self._preloader_compute_mapping:
            return

        for preloader in preloaders:
            if preloader.task_type in self._all_preload_tasks:
                preloader_module = importlib.import_module(preloader.module)
                self._logger.debug('Imported preloader module: {}', preloader_module)
                preloader_class = getattr(preloader_module, preloader.class_name)
                self._preloader_compute_mapping[preloader.task_type] = preloader_class
                self._logger.debug(
                    'Imported preloader class {} against preloader module {} for task type {}',
                    preloader_class,
                    preloader_module,
                    preloader.task_type,
                )

    async def _init_protocol_meta(self):
        """
        Initializes the protocol metadata by fetching the source chain epoch size and source chain ID.
        """
        protocol_abi = read_json_file(settings.protocol_state.abi, self._logger)
        self._protocol_state_contract = self._anchor_rpc_helper.get_current_node()['web3_client'].eth.contract(
            address=to_checksum_address(
                settings.protocol_state.address,
            ),
            abi=protocol_abi,
        )
        try:
            source_block_time = self._protocol_state_contract.functions.SOURCE_CHAIN_BLOCK_TIME(
                Web3.to_checksum_address(settings.data_market),
            ).call()
        except Exception as e:
            self._logger.exception(
                'Exception in querying protocol state for source chain block time: {}',
                e,
            )
            sys.exit(1)
        else:
            self._source_chain_block_time = source_block_time / 10 ** 4
            self._logger.debug('Set source chain block time to {}', self._source_chain_block_time)

        try:
            epoch_size = self._protocol_state_contract.functions.EPOCH_SIZE(
                Web3.to_checksum_address(settings.data_market),
            ).call()
        except Exception as e:
            self._logger.exception(
                'Exception in querying protocol state for epoch size: {}',
                e,
            )
            sys.exit(1)
        else:
            self._epoch_size = epoch_size
            self._logger.debug('Set epoch size to {}', self._epoch_size)
        self._epochs_in_a_day = 86400 // (self._epoch_size * self._source_chain_block_time)
        self._logger.debug('Set epochs in a day to {}', self._epochs_in_a_day)
        self._source_chain_epoch_size = await get_source_chain_epoch_size(
            redis_conn=self._redis_conn,
            state_contract_obj=self._protocol_state_contract,
            rpc_helper=self._anchor_rpc_helper,
        )
        self._source_chain_id = await get_source_chain_id(
            redis_conn=self._redis_conn,
            rpc_helper=self._anchor_rpc_helper,
            state_contract_obj=self._protocol_state_contract,
        )

    async def init_worker(self):
        """
        Initializes the worker by initializing the Redis pool, RPC helper, loading project metadata,
        initializing the RabbitMQ connection, and initializing the preloader compute mapping.
        """
        if not self._initialized:
            await self._init_redis_pool()
            self._logger.debug('Initialized Redis pool in Processor Distributor init_worker')
            await self._init_httpx_client()
            self._logger.debug('Initialized httpx client in Processor Distributor init_worker')
            await self._init_rpc_helper()
            self._logger.debug('Initialized RPC helper in Processor Distributor init_worker')
            await self._init_rabbitmq_connection()
            self._logger.debug('Initialized RabbitMQ connection in Processor Distributor init_worker')
            await self._init_preloader_compute_mapping()
            self._logger.debug('Initialized preloader compute mapping in Processor Distributor init_worker')
            await self._init_protocol_meta()
            asyncio.create_task(self._cleanup_tasks())

        self._initialized = True

    async def _get_proc_hub_start_time(self) -> int:
        """
        Retrieves the start time of the process hub core from Redis.

        Returns:
            int: The start time of the process hub core, or 0 if not found.
        """
        _ = await self._redis_conn.get(process_hub_core_start_timestamp())
        if _:
            return int(_)
        else:
            return 0

    async def _preloader_waiter(
        self,
        epoch: EpochBase,
    ):
        """
        Wait for all preloading tasks to complete for the given epoch, and distribute snapshot build tasks if all preloading
        dependencies are satisfied.

        Args:
            epoch: The epoch for which to wait for preloading tasks to complete.

        Returns:
            None
        """
        preloader_types_l = list(self._preload_completion_conditions[epoch.epochId].keys())
        conditions: List[Awaitable] = [
            self._preload_completion_conditions[epoch.epochId][k]
            for k in preloader_types_l
        ]
        self._logger.info(
            'Waiting for preload conditions against epoch {}: {}',
            epoch.epochId,
            conditions,
        )
        preload_results = await asyncio.gather(
            *conditions,
            return_exceptions=True,
        )
        succesful_preloads = list()
        failed_preloads = list()
        self._logger.debug(
            'Preloading asyncio gather returned with results {} for epoch {}',
            preload_results,
            epoch.epochId,
        )
        for i, preload_result in enumerate(preload_results):
            if isinstance(preload_result, Exception):
                self._logger.error(
                    f'Preloading failed for epoch {epoch.epochId} project type {preloader_types_l[i]}',
                )
                failed_preloads.append(preloader_types_l[i])
            else:
                succesful_preloads.append(preloader_types_l[i])
                self._logger.debug(
                    'Preloading successful for preloader {} for epoch {}',
                    preloader_types_l[i],
                    epoch.epochId,
                )

        self._logger.debug('Final list of successful preloads: {} for epoch {}', succesful_preloads, epoch.epochId)
        for project_type in self._project_type_config_mapping:
            project_config = self._project_type_config_mapping[project_type]
            if not project_config.preload_tasks:
                continue
            self._logger.debug(
                'Expected list of successful preloading for project type {} epoch {}: {}',
                project_type,
                epoch.epochId,
                project_config.preload_tasks,
            )
            if all([t in succesful_preloads for t in project_config.preload_tasks]):
                self._logger.info(
                    'Preloading dependency satisfied for project type {} epoch {}. Distributing snapshot build tasks...',
                    project_type, epoch.epochId,
                )
                await self._redis_conn.hset(
                    name=epoch_id_project_to_state_mapping(epoch.epochId, SnapshotterStates.PRELOAD.value),
                    mapping={
                        project_type: SnapshotterStateUpdate(
                            status='success', timestamp=int(time.time()),
                        ).json(),
                    },
                )
                await self._distribute_callbacks_snapshotting(project_type, epoch)
            else:
                self._logger.error(
                    'Preloading dependency not satisfied for project type {} epoch {}. Not distributing snapshot build tasks...',
                    project_type, epoch.epochId,
                )
                await self._redis_conn.hset(
                    name=epoch_id_project_to_state_mapping(epoch.epochId, SnapshotterStates.PRELOAD.value),
                    mapping={
                        project_type: SnapshotterStateUpdate(
                            status='failed', timestamp=int(time.time()),
                        ).json(),
                    },
                )
        # TODO: set separate overall status for failed and successful preloads
        if epoch.epochId in self._preload_completion_conditions:
            del self._preload_completion_conditions[epoch.epochId]

    async def _exec_preloaders(
        self, msg_obj: EpochBase,
    ):
        """
        Executes preloading tasks for the given epoch object.

        Args:
            msg_obj (EpochBase): The epoch object for which preloading tasks need to be executed.

        Returns:
            None
        """
        # Cleanup previous preloading complete tasks and events
        # Start all preload tasks
        self._logger.debug('Starting all preload tasks for epoch {}: {}', msg_obj.epochId, self._all_preload_tasks)
        for preloader in preloaders:
            if preloader.task_type in self._all_preload_tasks:
                preloader_class = self._preloader_compute_mapping[preloader.task_type]
                preloader_obj = preloader_class()
                preloader_compute_kwargs = dict(
                    epoch=msg_obj,
                    redis_conn=self._redis_conn,
                    rpc_helper=self._rpc_helper,
                )
                self._logger.debug(
                    'Starting preloader obj {} for epoch {}',
                    preloader.task_type,
                    msg_obj.epochId,
                )
                f = preloader_obj.compute(**preloader_compute_kwargs)
                self._preload_completion_conditions[msg_obj.epochId][preloader.task_type] = f
                self._logger.debug(
                    'Preloader future {} against task type {} for epoch {} started',
                    f,
                    preloader.task_type,
                    msg_obj.epochId,
                )
        for project_type, project_config in self._project_type_config_mapping.items():
            if not project_config.preload_tasks:
                # Release for snapshotting
                current_time = time.time()
                task = asyncio.create_task(
                    self._distribute_callbacks_snapshotting(
                        project_type, msg_obj,
                    ),
                )
                self._active_tasks.add((current_time, task))
                task.add_done_callback(lambda _: self._active_tasks.discard((current_time, task)))
                continue

        current_time = time.time()
        preloader_task = asyncio.create_task(
            self._preloader_waiter(
                epoch=msg_obj,
            ),
        )
        self._active_tasks.add((current_time, preloader_task))
        preloader_task.add_done_callback(lambda _: self._active_tasks.discard((current_time, preloader_task)))

    async def _epoch_release_processor(self, message: IncomingMessage):
        """
        This method is called when an epoch is released. It enables pending projects for the epoch and executes preloaders.

        Args:
            message (IncomingMessage): The message containing the epoch information.
        """
        try:
            msg_obj: EpochBase = (
                EpochBase.parse_raw(message.body)
            )
        except ValidationError:
            self._logger.opt(exception=settings.logs.debug_mode).error(
                'Bad message structure of epoch callback',
            )
            return
        except Exception:
            self._logger.opt(exception=settings.logs.debug_mode).error(
                'Unexpected message format of epoch callback',
            )
            return

        self._newly_added_projects = self._newly_added_projects.union(
            await self._enable_pending_projects_for_epoch(msg_obj.epochId),
        )
        self._logger.debug('Newly added projects for epoch {}: {}', msg_obj.epochId, self._newly_added_projects)
        self._logger.debug('Pushing epoch release to preloader coroutine: {}', msg_obj)
        current_time = time.time()
        task = asyncio.create_task(
            self._exec_preloaders(msg_obj=msg_obj),
        )
        self._active_tasks.add((current_time, task))
        task.add_done_callback(lambda _: self._active_tasks.discard((current_time, task)))

    async def _distribute_callbacks_snapshotting(self, project_type: str, epoch: EpochBase):
        """
        Distributes callbacks for snapshotting to the appropriate snapshotters based on the project type and epoch.

        Args:
            project_type (str): The type of project.
            epoch (EpochBase): The epoch to snapshot.

        Returns:
            None
        """
        # Send to snapshotters to get the balances of the addresses
        queuing_tasks = []

        async with self._rmq_channel_pool.acquire() as ch:
            # Prepare a message to send
            exchange = await ch.get_exchange(
                name=self._callback_exchange_name,
            )

            project_config = self._project_type_config_mapping[project_type]

            # Handling bulk mode projects
            if project_config.bulk_mode:
                process_unit = PowerloomSnapshotProcessMessage(
                    begin=epoch.begin,
                    end=epoch.end,
                    epochId=epoch.epochId,
                    bulk_mode=True,
                )

                msg_body = Message(process_unit.json().encode('utf-8'))
                await exchange.publish(
                    routing_key=f'powerloom-backend-callback:{settings.namespace}'
                    f':{settings.instance_id}:EpochReleased.{project_type}',
                    message=msg_body,
                )

                self._logger.info(
                    'Sent out message to be processed by worker'
                    f' {project_type} : {process_unit}',
                )
                return
            # Handling projects with no data sources
            if project_config.projects is None:
                project_id = f'{project_type}:{settings.namespace}'
                if project_id.lower() in self._newly_added_projects:
                    genesis = True
                    self._newly_added_projects.remove(project_id.lower())
                else:
                    genesis = False
                process_unit = PowerloomSnapshotProcessMessage(
                    begin=epoch.begin,
                    end=epoch.end,
                    epochId=epoch.epochId,
                    genesis=genesis,
                )

                msg_body = Message(process_unit.json().encode('utf-8'))
                await exchange.publish(
                    routing_key=f'powerloom-backend-callback:{settings.namespace}'
                    f':{settings.instance_id}:EpochReleased.{project_type}',
                    message=msg_body,
                )
                self._logger.info(
                    'Sent out message to be processed by worker'
                    f' {project_type} : {process_unit}',
                )
                return
            static_source_project_ids = list()
            # Handling projects with data sources
            for project in project_config.projects:
                project_id = f'{project_type}:{project}:{settings.namespace}'
                static_source_project_ids.append(project_id)
                if project_id.lower() in self._newly_added_projects:
                    genesis = True
                    self._newly_added_projects.remove(project_id.lower())
                else:
                    genesis = False

                data_sources = project.split('_')
                if len(data_sources) == 1:
                    data_source = data_sources[0]
                    primary_data_source = None
                else:
                    primary_data_source, data_source = data_sources
                process_unit = PowerloomSnapshotProcessMessage(
                    begin=epoch.begin,
                    end=epoch.end,
                    epochId=epoch.epochId,
                    data_source=data_source,
                    primary_data_source=primary_data_source,
                    genesis=genesis,
                )

                msg_body = Message(process_unit.json().encode('utf-8'))
                queuing_tasks.append(
                    exchange.publish(
                        routing_key=f'powerloom-backend-callback:{settings.namespace}'
                        f':{settings.instance_id}:EpochReleased.{project_type}',
                        message=msg_body,
                    ),
                )

            results = await asyncio.gather(*queuing_tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    self._logger.error(
                        'Error while sending message to queue. Error - {}',
                        result,
                    )
            self._logger.info(
                f'Sent out {len(project_config.projects)} messages to be processed by snapshot builder worker'
                f' for epoch {epoch.epochId}',
            )

    async def _enable_pending_projects_for_epoch(self, epoch_id) -> Set[str]:
        """
        Enables pending projects for the given epoch ID and returns a set of project IDs that were allowed.

        Args:
            epoch_id: The epoch ID for which to enable pending projects.

        Returns:
            A set of project IDs that were allowed.
        """
        pending_project_msgs: List[PowerloomProjectsUpdatedMessage] = self._upcoming_project_changes.pop(epoch_id, [])
        if not pending_project_msgs:
            return set()
        else:
            for msg_obj in pending_project_msgs:
                # Update projects list
                for project_type, project_config in self._project_type_config_mapping.items():
                    projects_set = set(project_config.projects)
                    if project_type in msg_obj.projectId:
                        if project_config.projects is None:
                            continue
                        data_source = msg_obj.projectId.split(':')[-2]
                        if msg_obj.allowed:
                            projects_set.add(data_source)
                        else:
                            if data_source in project_config.projects:
                                projects_set.discard(data_source)
                    project_config.projects = list(projects_set)

        return set([msg.projectId.lower() for msg in pending_project_msgs if msg.allowed])

    def _fetch_base_project_list(self, project_type: str) -> List[str]:
        """
        Fetches the base project list for the given project type.

        Args:
            project_type (str): The project type.

        Returns:
            List[str]: The base project list.
        """
        if project_type in self._project_type_config_mapping:
            return self._project_type_config_mapping[project_type].projects
        # another sigle based aggregate project
        else:
            base_project_type = self._aggregator_config_mapping[project_type].base_project_type
            return self._fetch_base_project_list(base_project_type)

    @lru_cache(maxsize=None)
    def _gen_projects_to_wait_for(self, project_type: str) -> List[str]:
        """
        Generates the projects to wait for based on the project type.

        Args:
            project_type (str): The project type.

        Returns:
            List[str]: The projects to wait for.
        """
        aggregator_config = self._aggregator_config_mapping[project_type]

        if aggregator_config.aggregate_on == AggregateOn.single_project:
            base_project_type = aggregator_config.base_project_type
            return set([f'{project_type}:{project}:{settings.namespace}' for project in self._fetch_base_project_list(base_project_type)])
        else:
            project_types_to_wait_for = aggregator_config.project_types_to_wait_for
            projects_to_wait_for = set()
            for project_type in project_types_to_wait_for:
                if project_type in self._project_type_config_mapping:
                    base_project_config = self._project_type_config_mapping[project_type]
                    projects_to_wait_for.update(
                        [f'{project_type}:{project}:{settings.namespace}' for project in base_project_config.projects],
                    )
                else:
                    projects_to_wait_for.update(self._gen_projects_to_wait_for(project_type))
            return projects_to_wait_for

    async def _update_all_projects(self, message: IncomingMessage):
        """
        Updates all projects based on the incoming message.

        Args:
            message (IncomingMessage): The incoming message containing the project updates.
        """

        event_type = message.routing_key.split('.')[-1]

        if event_type == 'ProjectsUpdated':
            msg_obj: PowerloomProjectsUpdatedMessage = (
                PowerloomProjectsUpdatedMessage.parse_raw(message.body)
            )
        else:
            return

        self._upcoming_project_changes[msg_obj.enableEpochId].append(msg_obj)

    # NOTE: refactored from V1 to not send snapshot finalized message to payload commit queue since the service is deprecated
    async def _cache_finalized_snapshot(self, message: IncomingMessage):
        """
        Caches the snapshot data and forwards it to the payload commit queue.

        Args:
            message (IncomingMessage): The incoming message containing the snapshot data.

        Returns:
            None
        """
        event_type = message.routing_key.split('.')[-1]

        if event_type == 'SnapshotFinalized':
            self._logger.debug(f'SnapshotFinalizedEvent caught with message {message}')
            msg_obj: PowerloomSnapshotFinalizedMessage = (
                PowerloomSnapshotFinalizedMessage.parse_raw(message.body)
            )
        else:
            return

        # set project last finalized epoch in redis
        await self._redis_conn.set(
            name=project_last_finalized_epoch_key(msg_obj.projectId),
            value=msg_obj.epochId,
            ex=60.0,
        )

        # Add to project finalized data zset
        await self._redis_conn.zadd(
            project_finalized_data_zset(project_id=msg_obj.projectId),
            {msg_obj.snapshotCid: msg_obj.epochId},
        )

        await self._redis_conn.hset(
            name=epoch_id_project_to_state_mapping(msg_obj.epochId, SnapshotterStates.SNAPSHOT_FINALIZE.value),
            mapping={
                msg_obj.projectId: SnapshotterStateUpdate(
                    status='success', timestamp=int(time.time()), extra={'snapshot_cid': msg_obj.snapshotCid},
                ).json(),
            },
        )

        self._logger.trace(f'Payload Commit Message Distribution time - {int(time.time())}')

    async def _distribute_callbacks_aggregate(self, message: IncomingMessage):
        """
        Distributes the callbacks for aggregation.

        :param message: IncomingMessage object containing the message to be processed.
        """
        event_type = message.routing_key.split('.')[-1]
        try:
            if event_type != 'SnapshotSubmitted':
                self._logger.error(f'Unknown event type {event_type}')
                return

            process_unit: PowerloomSnapshotSubmittedMessage = (
                PowerloomSnapshotSubmittedMessage.parse_raw(message.body)
            )

        except ValidationError:
            self._logger.opt(exception=settings.logs.debug_mode).error(
                'Bad message structure of event callback',
            )
            return
        except Exception:
            self._logger.opt(exception=settings.logs.debug_mode).error(
                'Unexpected message format of event callback',
            )
            return
        self._logger.trace(f'Aggregation Task Distribution time - {int(time.time())}')

        # go through aggregator config, if it matches then send appropriate message
        rabbitmq_publish_tasks = list()
        async with self._rmq_channel_pool.acquire() as channel:
            exchange = await channel.get_exchange(
                name=self._callback_exchange_name,
            )
            for config in aggregator_config:
                task_type = config.project_type
                if config.aggregate_on == AggregateOn.single_project:
                    if config.base_project_type not in process_unit.projectId:
                        self._logger.trace(f'projectId mismatch {process_unit.projectId} {config.base_project_type}')
                        continue

                    rabbitmq_publish_tasks.append(
                        exchange.publish(
                            routing_key=f'powerloom-backend-callback:{settings.namespace}:'
                            f'{settings.instance_id}:CalculateAggregate.{task_type}',
                            message=Message(process_unit.json().encode('utf-8')),
                        ),
                    )
                elif config.aggregate_on == AggregateOn.multi_project:
                    projects_to_wait_for = self._gen_projects_to_wait_for(config.project_type)
                    if process_unit.projectId not in projects_to_wait_for:
                        self._logger.trace(
                            f'projectId not required for {config.project_type}: {process_unit.projectId}',
                        )
                        continue

                    # cleanup redis for all previous epochs (5 buffer)
                    await self._redis_conn.zremrangebyscore(
                        f'powerloom:aggregator:{config.project_type}:events',
                        0,
                        process_unit.epochId - 5,
                    )

                    await self._redis_conn.zadd(
                        f'powerloom:aggregator:{config.project_type}:events',
                        {process_unit.json(): process_unit.epochId},
                    )

                    events = await self._redis_conn.zrangebyscore(
                        f'powerloom:aggregator:{config.project_type}:events',
                        process_unit.epochId,
                        process_unit.epochId,
                    )

                    if not events:
                        self._logger.debug(f'No events found for {process_unit.epochId}')
                        continue

                    event_project_ids = set()
                    finalized_messages = list()

                    for event in events:
                        event = PowerloomSnapshotSubmittedMessage.parse_raw(event)
                        if event.projectId not in event_project_ids:
                            event_project_ids.add(event.projectId)
                            finalized_messages.append(event)

                    if event_project_ids == projects_to_wait_for:
                        self._logger.info(
                            f'All project snapshots accumulated for epoch {process_unit.epochId} against multi aggregate project type {config.project_type}, aggregating',
                        )
                        final_msg = PowerloomCalculateAggregateMessage(
                            messages=sorted(finalized_messages, key=lambda x: x.projectId),
                            epochId=process_unit.epochId,
                            timestamp=int(time.time()),
                        )

                        rabbitmq_publish_tasks.append(
                            exchange.publish(
                                routing_key=f'powerloom-backend-callback:{settings.namespace}'
                                f':{settings.instance_id}:CalculateAggregate.{task_type}',
                                message=Message(final_msg.json().encode('utf-8')),
                            ),
                        )

                        # Cleanup redis for current epoch

                        await self._redis_conn.zremrangebyscore(
                            f'powerloom:aggregator:{config.project_type}:events',
                            process_unit.epochId,
                            process_unit.epochId,
                        )

                    else:
                        self._logger.trace(
                            f'Not all projects present for epoch {process_unit.epochId} against multi aggregate project type {config.project_type},'
                            f' {len(projects_to_wait_for) - len(event_project_ids)} missing',
                        )
        await asyncio.gather(*rabbitmq_publish_tasks, return_exceptions=True)

    async def _cleanup_older_epoch_status(self, epoch_id: int):
        """
        Deletes the epoch status keys for the epoch that is 30 epochs older than the given epoch_id.
        """
        tasks = [self._redis_conn.delete(epoch_id_epoch_released_key(epoch_id - 30))]
        delete_keys = list()
        for state in SnapshotterStates:
            k = epoch_id_project_to_state_mapping(epoch_id - 30, state.value)
            delete_keys.append(k)
        if delete_keys:
            tasks.append(self._redis_conn.delete(*delete_keys))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _on_rabbitmq_message(self, message: IncomingMessage):
        """
        Callback function to handle incoming RabbitMQ messages.

        Args:
            message (IncomingMessage): The incoming RabbitMQ message.

        Returns:
            None
        """
        await message.ack()

        message_type = message.routing_key.split('.')[-1]
        self._logger.info(
            (
                'Got message to process and distribute: {}'
            ),
            message.body,
        )

        if message_type == 'EpochReleased':
            try:
                epoch_msg: EpochBase = EpochBase.parse_raw(message.body)
            except:
                pass
            else:
                await self._redis_conn.set(
                    epoch_id_epoch_released_key(epoch_msg.epochId),
                    int(time.time()),
                )
                current_time = time.time()
                task = asyncio.create_task(
                    self._cleanup_older_epoch_status(epoch_msg.epochId),
                )
                self._active_tasks.add((current_time, task))
                task.add_done_callback(lambda _: self._active_tasks.discard((current_time, task)))

            _ = await self._redis_conn.get(active_status_key)
            if _:
                active_status = bool(int(_))
                if not active_status:
                    self._logger.error('System is not active, ignoring released Epoch')
                else:
                    await self._epoch_release_processor(message)

        elif message_type == 'SnapshotSubmitted':
            await self._distribute_callbacks_aggregate(
                message,
            )

        elif message_type == 'SnapshotFinalized':
            self._logger.debug(f'SnapshotFinalizedEvent caught with message {message}')
            await self._cache_finalized_snapshot(
                message,
            )
        elif message_type == 'ProjectsUpdated':
            await self._update_all_projects(message)
        elif message_type == 'allSnapshottersUpdated':
            msg_cast = SnapshottersUpdatedEvent.parse_raw(message.body)
            if msg_cast.snapshotterAddress == to_checksum_address(settings.instance_id):
                if self._redis_conn:
                    await self._redis_conn.set(
                        active_status_key,
                        int(msg_cast.allowed),
                    )
        else:
            self._logger.error(
                (
                    'Unknown routing key for callback distribution: {}'
                ),
                message.routing_key,
            )

        if self._redis_conn:
            await self._redis_conn.close()

    async def _rabbitmq_consumer(self, loop):
        """
        Consume messages from a RabbitMQ queue.

        Args:
            loop: The event loop to use for the consumer.

        Returns:
            None
        """
        async with self._rmq_channel_pool.acquire() as channel:
            await channel.set_qos(10)
            exchange = await channel.get_exchange(
                name=self._consume_exchange_name,
            )
            q_obj = await channel.get_queue(
                name=self._consume_queue_name,
                ensure=False,
            )
            self._logger.debug(
                f'Consuming queue {self._consume_queue_name} with routing key {self._consume_queue_routing_key}...',
            )
            await q_obj.bind(exchange, routing_key=self._consume_queue_routing_key)
            await q_obj.consume(self._on_rabbitmq_message)

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

    def run(self) -> None:
        """
        Runs the ProcessorDistributor by setting resource limits, registering signal handlers,
        initializing the worker, starting the RabbitMQ consumer, and running the event loop.
        """
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )
        for signame in [SIGINT, SIGTERM, SIGQUIT]:
            signal(signame, self._signal_handler)
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        ev_loop = asyncio.get_event_loop()
        ev_loop.run_until_complete(self.init_worker())

        self._logger.debug('Starting RabbitMQ consumer on queue {} for Processor Distributor', self._consume_queue_name)
        self._core_rmq_consumer = asyncio.ensure_future(
            self._rabbitmq_consumer(ev_loop),
        )
        try:
            ev_loop.run_forever()
        finally:
            ev_loop.close()


if __name__ == '__main__':
    processor_distributor = ProcessorDistributor('ProcessorDistributor')
    processor_distributor.run()
