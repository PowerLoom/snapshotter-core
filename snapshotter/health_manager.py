import asyncio
import json
import multiprocessing
import resource
import time
from datetime import datetime
from functools import partial
from typing import Set

import uvloop
from aio_pika import Message
from aio_pika.pool import Pool
from httpx import AsyncClient
from httpx import AsyncHTTPTransport
from httpx import Limits
from httpx import Timeout
from redis import asyncio as aioredis

from snapshotter.settings.config import settings
from snapshotter.utils.callback_helpers import get_rabbitmq_channel
from snapshotter.utils.callback_helpers import get_rabbitmq_robust_connection_async
from snapshotter.utils.callback_helpers import send_failure_notifications_async
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.data_models import SnapshotterEpochProcessingReportItem
from snapshotter.utils.models.data_models import SnapshotterIssue
from snapshotter.utils.models.data_models import SnapshotterReportState
from snapshotter.utils.models.data_models import SnapshotterStates
from snapshotter.utils.models.data_models import SnapshotterStateUpdate
from snapshotter.utils.models.message_models import ProcessHubCommand
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import epoch_id_project_to_state_mapping
from snapshotter.utils.redis.redis_keys import last_epoch_detected_epoch_id_key
from snapshotter.utils.redis.redis_keys import last_epoch_detected_timestamp_key
from snapshotter.utils.redis.redis_keys import last_snapshot_processing_complete_timestamp_key
from snapshotter.utils.redis.redis_keys import process_hub_core_start_timestamp


class HealthManager(multiprocessing.Process):
    """
    A class responsible for managing the health of the snapshotter system.

    This class performs periodic health checks, monitors epoch processing,
    and can initiate respawn commands when issues are detected.
    """

    _aioredis_pool: RedisPoolCache
    _redis_conn: aioredis.Redis
    _async_transport: AsyncHTTPTransport
    _client: AsyncClient

    def __init__(self, name, **kwargs):
        """
        Initialize the HealthManager object.

        Args:
            name (str): The name of the HealthManager.
            **kwargs: Additional keyword arguments.

        Attributes:
            _rabbitmq_interactor: The RabbitMQ interactor object.
            _shutdown_initiated (bool): Flag indicating if shutdown has been initiated.
            _initialized (bool): Flag indicating if the HealthManager has been initialized.
            _last_epoch_processing_health_check (int): Timestamp of the last epoch processing health check.
            _last_epoch_checked (int): The last epoch ID that was checked.
        """
        super(HealthManager, self).__init__(name=name, **kwargs)
        self._rabbitmq_interactor = None
        self._shutdown_initiated = False
        self._initialized = False
        self._last_epoch_processing_health_check = 0
        self._last_epoch_checked = 0
        # Task tracking
        self._active_tasks: Set[asyncio.Task] = set()
        self._task_timeout = settings.async_task_config.task_timeout
        self._task_cleanup_interval = settings.async_task_config.task_cleanup_interval

    async def _init_redis_pool(self):
        """
        Initializes the Redis connection pool and populates it with connections.
        """
        self._aioredis_pool = RedisPoolCache()
        await self._aioredis_pool.populate()
        self._redis_conn = self._aioredis_pool._aioredis_pool

    async def _init_rabbitmq_connection(self):
        """
        Initializes the RabbitMQ connection pool and channel pool.

        The RabbitMQ connection pool is used to manage a pool of connections to the RabbitMQ server,
        while the channel pool is used to manage a pool of channels for each connection.
        """
        self._rmq_connection_pool = Pool(
            get_rabbitmq_robust_connection_async,
            max_size=2, loop=asyncio.get_event_loop(),
        )
        self._rmq_channel_pool = Pool(
            partial(get_rabbitmq_channel, self._rmq_connection_pool), max_size=10,
            loop=asyncio.get_event_loop(),
        )

    async def _init_httpx_client(self):
        """
        Initializes the HTTPX client with the specified settings.

        This method sets up an AsyncHTTPTransport with custom limits and timeouts,
        and creates an AsyncClient for making HTTP requests.
        """
        self._async_transport = AsyncHTTPTransport(
            limits=Limits(
                max_connections=100,
                max_keepalive_connections=50,
                keepalive_expiry=None,
            ),
        )
        self._client = AsyncClient(
            base_url=settings.reporting.service_url,
            timeout=Timeout(
                pool=settings.httpx.pool_timeout,
                connect=settings.httpx.connect_timeout,
                read=settings.httpx.read_timeout,
                write=settings.httpx.write_timeout,
            ),
            follow_redirects=False,
            transport=self._async_transport,
        )

    async def _send_proc_hub_respawn(self):
        """
        Sends a respawn command to the process hub.

        This method creates a ProcessHubCommand object with the command 'respawn',
        acquires a channel from the channel pool, gets the exchange, and publishes
        the command message to the exchange.
        """
        proc_hub_cmd = ProcessHubCommand(
            command='respawn',
        )
        async with self._rmq_channel_pool.acquire() as channel:
            exchange = await channel.get_exchange(
                name=f'{settings.rabbitmq.setup.core.exchange}:{settings.namespace}',
            )
            await exchange.publish(
                routing_key=f'processhub-commands:{settings.namespace}:{settings.instance_id}',
                message=Message(proc_hub_cmd.json().encode('utf-8')),
            )

    async def init_worker(self):
        """
        Initializes the worker by setting up Redis, RabbitMQ, and HTTP client connections.

        This method is called once to set up all necessary connections and pools
        before the HealthManager starts its main loop.
        """
        if not self._initialized:
            await self._init_redis_pool()
            await self._init_httpx_client()
            await self._init_rabbitmq_connection()
            asyncio.create_task(self._cleanup_tasks())
            self._initialized = True

    async def _get_proc_hub_start_time(self) -> int:
        """
        Retrieves the start time of the process hub core from Redis.

        Returns:
            int: The start time of the process hub core, or 0 if not found.
        """
        _ = await self._redis_conn.get(process_hub_core_start_timestamp())
        return int(_) if _ else 0

    async def _epoch_processing_health_check(self, current_epoch_id):
        """
        Perform health check for epoch processing.

        This method checks various conditions to ensure that epoch processing
        is functioning correctly. It may send failure notifications or respawn
        commands if issues are detected.

        Args:
            current_epoch_id (int): The current epoch ID.
        """
        # Skip check if current epoch is too low
        if current_epoch_id < 5:
            return

        # Get last set start time by proc hub core
        start_time = await self._get_proc_hub_start_time()

        # Only start if 5 minutes have passed since proc hub core start time
        if int(time.time()) - start_time < 5 * 60:
            self._logger.info(
                'Skipping epoch processing health check because 5 minutes have not passed since proc hub core start time',
            )
            return

        if start_time == 0:
            self._logger.info('Skipping epoch processing health check because proc hub start time is not set')
            return

        # Only run once every minute
        if (
            self._last_epoch_processing_health_check != 0 and
            int(time.time()) - self._last_epoch_processing_health_check < 60
        ):
            self._logger.debug(
                'Skipping epoch processing health check because it was run less than a minute ago',
            )
            return

        self._last_epoch_processing_health_check = int(time.time())

        # Fetch last epoch detected and last snapshot processed timestamps
        last_epoch_detected = await self._redis_conn.get(last_epoch_detected_timestamp_key())
        last_snapshot_processed = await self._redis_conn.get(last_snapshot_processing_complete_timestamp_key())

        if last_epoch_detected:
            last_epoch_detected = int(last_epoch_detected)

        if last_snapshot_processed:
            last_snapshot_processed = int(last_snapshot_processed)

        # Handle first run or no new epoch detected
        if self._last_epoch_checked == 0:
            self._logger.info(
                'Skipping epoch processing health check because this is the first run with no reference of last epoch checked by this service',
            )
            self._last_epoch_checked = current_epoch_id
            return
        elif current_epoch_id == self._last_epoch_checked:
            self._logger.info(
                'Skipping epoch processing health check because no new epoch was detected since last check. Probably Prost network is not producing new blocks',
            )
            await send_failure_notifications_async(
                client=self._client,
                message=SnapshotterIssue(
                    instanceID=settings.instance_id,
                    issueType=SnapshotterReportState.UNHEALTHY_EPOCH_PROCESSING.value,
                    projectID='',
                    epochId='',
                    timeOfReporting=datetime.now().isoformat(),
                    extra=json.dumps(
                        {
                            'last_epoch_checked': self._last_epoch_checked,
                            'current_epoch_id': current_epoch_id,
                            'reason': 'No new epoch detected since last check. Probably Prost network is not producing new blocks',
                        },
                    ),
                ),
            )
            return

        # Check if no epoch is detected for 5 minutes
        if last_epoch_detected and int(time.time()) - last_epoch_detected > 5 * 60:
            self._logger.debug(
                'Sending unhealthy epoch report to reporting service due to no epoch detected for ~30 epochs',
            )
            await send_failure_notifications_async(
                client=self._client,
                message=SnapshotterIssue(
                    instanceID=settings.instance_id,
                    issueType=SnapshotterReportState.UNHEALTHY_EPOCH_PROCESSING.value,
                    projectID='',
                    epochId='',
                    timeOfReporting=datetime.now().isoformat(),
                    extra=json.dumps(
                        {
                            'last_epoch_detected': last_epoch_detected,
                        },
                    ),
                ),
            )
            self._logger.info(
                'Sending respawn command for all process hub core children because no epoch was detected for ~30 epochs',
            )
            await self._send_proc_hub_respawn()
            self._last_epoch_checked = current_epoch_id
            return

        # Check if time difference between last epoch detected and last snapshot processed is too large
        if (
            last_epoch_detected and last_snapshot_processed and
            last_epoch_detected - last_snapshot_processed > 5 * 60
        ):
            self._logger.debug(
                'Sending unhealthy epoch report to reporting service due to no snapshot processing for ~30 epochs',
            )
            await send_failure_notifications_async(
                client=self._client,
                message=SnapshotterIssue(
                    instanceID=settings.instance_id,
                    issueType=SnapshotterReportState.UNHEALTHY_EPOCH_PROCESSING.value,
                    projectID='',
                    epochId='',
                    timeOfReporting=datetime.now().isoformat(),
                    extra=json.dumps(
                        {
                            'last_epoch_detected': last_epoch_detected,
                            'last_snapshot_processed': last_snapshot_processed,
                        },
                    ),
                ),
            )
            self._logger.info(
                'Sending respawn command for all process hub core children because no snapshot processing was done for ~30 epochs',
            )
            await self._send_proc_hub_respawn()
            self._last_epoch_checked = current_epoch_id
            return
        else:
            # Check for epoch processing status
            epoch_health = dict()
            # Check from previous epoch processing status until 2 further epochs
            build_state_val = SnapshotterStates.SNAPSHOT_BUILD.value
            for epoch_id in range(current_epoch_id - 1, current_epoch_id - 3 - 1, -1):
                epoch_specific_report = SnapshotterEpochProcessingReportItem.construct()
                success_percentage = 0
                epoch_specific_report.epochId = epoch_id
                state_report_entries = await self._redis_conn.hgetall(
                    name=epoch_id_project_to_state_mapping(epoch_id=epoch_id, state_id=build_state_val),
                )
                if state_report_entries:
                    project_state_report_entries = {
                        project_id.decode('utf-8'): SnapshotterStateUpdate.parse_raw(project_state_entry)
                        for project_id, project_state_entry in state_report_entries.items()
                    }
                    epoch_specific_report.transitionStatus[build_state_val] = project_state_report_entries
                    success_percentage += len(
                        [
                            project_state_report_entry
                            for project_state_report_entry in project_state_report_entries.values()
                            if project_state_report_entry.status == 'success'
                        ],
                    ) / len(project_state_report_entries)

                if any([x is None for x in epoch_specific_report.transitionStatus.values()]):
                    epoch_health[epoch_id] = False
                    self._logger.debug(
                        'Marking epoch {} as unhealthy due to missing state reports against transitions {}',
                        epoch_id,
                        [x for x, y in epoch_specific_report.transitionStatus.items() if y is None],
                    )
                if success_percentage < 0.5 and success_percentage != 0:
                    epoch_health[epoch_id] = False
                    self._logger.debug(
                        'Marking epoch {} as unhealthy due to low success percentage: {}',
                        epoch_id,
                        success_percentage,
                    )
            if len([epoch_id for epoch_id, healthy in epoch_health.items() if not healthy]) >= 2:
                self._logger.debug(
                    'Sending unhealthy epoch report to reporting service: {}',
                    epoch_health,
                )
                await send_failure_notifications_async(
                    client=self._client,
                    message=SnapshotterIssue(
                        instanceID=settings.instance_id,
                        issueType=SnapshotterReportState.UNHEALTHY_EPOCH_PROCESSING.value,
                        projectID='',
                        epochId='',
                        timeOfReporting=datetime.now().isoformat(),
                        extra=json.dumps(
                            {
                                'epoch_health': epoch_health,
                            },
                        ),
                    ),
                )
                self._logger.info(
                    'Sending respawn command for all process hub core children because epochs were found unhealthy: {}',
                    epoch_health,
                )
                await self._send_proc_hub_respawn()
        self._last_epoch_checked = current_epoch_id

    async def _check_health(self, loop):
        """
        Continuously checks the health of the system.

        This method runs in a loop, performing health checks every 60 seconds.
        It will break the loop if a shutdown is initiated.

        Args:
            loop (asyncio.AbstractEventLoop): The event loop to use for scheduling tasks.
        """
        while True:
            await asyncio.sleep(60)
            if self._shutdown_initiated:
                break

            last_detected_epoch = await self._redis_conn.get(last_epoch_detected_epoch_id_key())
            if last_detected_epoch:
                last_detected_epoch = int(last_detected_epoch)
                current_time = time.time()
                task = asyncio.create_task(self._epoch_processing_health_check(last_detected_epoch))
                self._active_tasks.add((current_time, task))
                task.add_done_callback(lambda _: self._active_tasks.discard((current_time, task)))

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
        Runs the HealthManager by setting resource limits, initializing the worker,
        and starting the health check loop.

        This method sets up the event loop, initializes all necessary components,
        and starts the main health check loop.
        """
        self._logger = logger.bind(
            module='HealthManager',
        )
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        ev_loop = asyncio.get_event_loop()
        ev_loop.run_until_complete(self.init_worker())

        self._logger.debug('Starting Health Manager')
        asyncio.ensure_future(
            self._check_health(ev_loop),
        )
        try:
            ev_loop.run_forever()
        finally:
            ev_loop.close()


if __name__ == '__main__':
    health_manager = HealthManager('HealthManager')
    health_manager.run()
