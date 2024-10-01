import asyncio
import importlib
import json
import time
from typing import Optional

from aio_pika import IncomingMessage
from ipfs_client.main import AsyncIPFSClient
from ipfs_client.main import AsyncIPFSClientSingleton
from pydantic import ValidationError

from snapshotter.settings.config import projects_config
from snapshotter.settings.config import settings
from snapshotter.utils.callback_helpers import send_failure_notifications_async
from snapshotter.utils.generic_worker import GenericAsyncWorker
from snapshotter.utils.models.data_models import SnapshotterIssue
from snapshotter.utils.models.data_models import SnapshotterReportState
from snapshotter.utils.models.data_models import SnapshotterStates
from snapshotter.utils.models.data_models import SnapshotterStateUpdate
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.redis.rate_limiter import load_rate_limiter_scripts
from snapshotter.utils.redis.redis_keys import epoch_id_project_to_state_mapping
from snapshotter.utils.redis.redis_keys import last_snapshot_processing_complete_timestamp_key
from snapshotter.utils.redis.redis_keys import submitted_base_snapshots_key


class SnapshotAsyncWorker(GenericAsyncWorker):
    """
    A worker class for asynchronous snapshot processing.

    This class extends GenericAsyncWorker and provides functionality for processing
    snapshot tasks asynchronously, including IPFS operations and project-specific calculations.
    """

    _ipfs_singleton: AsyncIPFSClientSingleton
    _ipfs_writer_client: AsyncIPFSClient
    _ipfs_reader_client: AsyncIPFSClient

    def __init__(self, name, **kwargs):
        """
        Initialize a SnapshotAsyncWorker instance.

        Args:
            name (str): The name of the worker.
            **kwargs: Additional keyword arguments to be passed to the GenericAsyncWorker constructor.
        """
        self._q = f'powerloom-backend-cb-snapshot:{settings.namespace}:{settings.instance_id}'
        self._rmq_routing = f'powerloom-backend-callback:{settings.namespace}:{settings.instance_id}:EpochReleased.*'
        super(SnapshotAsyncWorker, self).__init__(name=name, **kwargs)
        self._project_calculation_mapping = None
        self._task_types = []
        for project_config in projects_config:
            task_type = project_config.project_type
            self._task_types.append(task_type)

    def _gen_project_id(self, task_type: str, data_source: Optional[str] = None, primary_data_source: Optional[str] = None):
        """
        Generate a project ID based on the given parameters.

        Args:
            task_type (str): The type of task.
            data_source (Optional[str]): The data source. Defaults to None.
            primary_data_source (Optional[str]): The primary data source. Defaults to None.

        Returns:
            str: The generated project ID.
        """
        if not data_source:
            # For generic use cases that don't have a data source like block details
            project_id = f'{task_type}:{settings.namespace}'
        else:
            if primary_data_source:
                project_id = f'{task_type}:{primary_data_source.lower()}_{data_source.lower()}:{settings.namespace}'
            else:
                project_id = f'{task_type}:{data_source.lower()}:{settings.namespace}'
        return project_id

    async def _process_single_mode(self, msg_obj: PowerloomSnapshotProcessMessage, task_type: str):
        """
        Process a single mode snapshot task.

        This method handles the computation, transformation, and storage of a single snapshot.

        Args:
            msg_obj (PowerloomSnapshotProcessMessage): The message object containing snapshot task details.
            task_type (str): The type of task to be performed.

        Raises:
            Exception: If an error occurs while processing the snapshot task.
        """
        project_id = self._gen_project_id(
            task_type=task_type,
            data_source=msg_obj.data_source,
            primary_data_source=msg_obj.primary_data_source,
        )

        try:
            # Get the task processor for the given task type
            task_processor = self._project_calculation_mapping[task_type]

            # Compute the snapshot
            snapshot = await task_processor.compute(
                epoch=msg_obj,
                redis_conn=self._redis_conn,
                rpc_helper=self._rpc_helper,
            )

            if snapshot is None:
                self._logger.debug(
                    'No snapshot data for: {}, skipping...', msg_obj,
                )

        except Exception as e:
            # Handle exceptions during snapshot processing
            self._logger.opt(exception=settings.logs.trace_enabled).error(
                'Exception processing callback for epoch: {}, Error: {},'
                'sending failure notifications', msg_obj, e,
            )

            # Prepare and send failure notification
            notification_message = SnapshotterIssue(
                instanceID=settings.instance_id,
                issueType=SnapshotterReportState.MISSED_SNAPSHOT.value,
                projectID=project_id,
                epochId=str(msg_obj.epochId),
                timeOfReporting=str(time.time()),
                extra=json.dumps({'issueDetails': f'Error : {e}'}),
            )

            await send_failure_notifications_async(
                client=self._client, message=notification_message,
            )

            # Update Redis with failure state
            await self._redis_conn.hset(
                name=epoch_id_project_to_state_mapping(
                    epoch_id=msg_obj.epochId, state_id=SnapshotterStates.SNAPSHOT_BUILD.value,
                ),
                mapping={
                    project_id: SnapshotterStateUpdate(
                        status='failed', error=str(e), timestamp=int(time.time()),
                    ).json(),
                },
            )
        else:
            # Handle successful snapshot processing
            p = self._redis_conn.pipeline()

            # Store the snapshot in Redis
            p.set(
                name=submitted_base_snapshots_key(
                    epoch_id=msg_obj.epochId, project_id=project_id,
                ),
                value=snapshot.json(),
                # Set expiration time (10 times the submission window * 2 seconds)
                ex=self._submission_window * 10 * 2,
            )

            # Update Redis with success state
            p.hset(
                name=epoch_id_project_to_state_mapping(
                    epoch_id=msg_obj.epochId, state_id=SnapshotterStates.SNAPSHOT_BUILD.value,
                ),
                mapping={
                    project_id: SnapshotterStateUpdate(
                        status='success', timestamp=int(time.time()),
                    ).json(),
                },
            )

            # Update last snapshot processing timestamp
            await self._redis_conn.set(
                name=last_snapshot_processing_complete_timestamp_key(),
                value=int(time.time()),
            )

            if not snapshot:
                self._logger.debug(
                    'No snapshot data for: {}, skipping...', msg_obj,
                )
                return

            # Execute Redis pipeline
            await p.execute()

            await self._commit_payload(
                task_type=task_type,
                project_id=project_id,
                epoch=msg_obj,
                snapshot=snapshot,
                storage_flag=settings.web3storage.upload_snapshots,
                _ipfs_writer_client=self._ipfs_writer_client,
            )

    async def _process_bulk_mode(self, msg_obj: PowerloomSnapshotProcessMessage, task_type: str):
        """
        Process snapshots in bulk mode.

        This method handles the computation and storage of multiple snapshots at once.

        Args:
            msg_obj (PowerloomSnapshotProcessMessage): The message object containing snapshot task details.
            task_type (str): The type of task to be performed.

        Raises:
            Exception: If an error occurs while processing the snapshots.
        """
        try:
            # Get the task processor for the given task type
            task_processor = self._project_calculation_mapping[task_type]

            # Compute snapshots in bulk
            snapshots = await task_processor.compute(
                epoch=msg_obj,
                redis_conn=self._redis_conn,
                rpc_helper=self._rpc_helper,
            )

            if not snapshots:
                self._logger.debug(
                    'No snapshot data for: {}, skipping...', msg_obj,
                )

        except Exception as e:
            # Handle exceptions during bulk snapshot processing
            self._logger.opt(exception=True).error(
                'Exception processing callback for epoch: {}, Error: {},'
                'sending failure notifications', msg_obj, e,
            )

            # Prepare and send failure notification
            notification_message = SnapshotterIssue(
                instanceID=settings.instance_id,
                issueType=SnapshotterReportState.MISSED_SNAPSHOT.value,
                projectID=f'{task_type}:{settings.namespace}',
                epochId=str(msg_obj.epochId),
                timeOfReporting=str(time.time()),
                extra=json.dumps({'issueDetails': f'Error : {e}'}),
            )

            await send_failure_notifications_async(
                client=self._client, message=notification_message,
            )

            # Update Redis with failure state
            await self._redis_conn.hset(
                name=epoch_id_project_to_state_mapping(
                    epoch_id=msg_obj.epochId, state_id=SnapshotterStates.SNAPSHOT_BUILD.value,
                ),
                mapping={
                    f'{task_type}:{settings.namespace}': SnapshotterStateUpdate(
                        status='failed', error=str(e), timestamp=int(time.time()),
                    ).json(),
                },
            )
        else:
            # Handle successful bulk snapshot processing
            await self._redis_conn.set(
                name=last_snapshot_processing_complete_timestamp_key(),
                value=int(time.time()),
            )

            if not snapshots:
                self._logger.debug(
                    'No snapshot data for: {}, skipping...', msg_obj,
                )
                return

            self._logger.info('Sending snapshots to commit service: {}', snapshots)

            # Process each snapshot in the bulk result
            for project_data_source, snapshot in snapshots:
                # Parse data sources
                data_sources = project_data_source.split('_')
                if len(data_sources) == 1:
                    data_source = data_sources[0]
                    primary_data_source = None
                else:
                    primary_data_source, data_source = data_sources

                # Generate project ID
                project_id = self._gen_project_id(
                    task_type=task_type, data_source=data_source, primary_data_source=primary_data_source,
                )

                # Store snapshot in Redis
                await self._redis_conn.set(
                    name=submitted_base_snapshots_key(
                        epoch_id=msg_obj.epochId, project_id=project_id,
                    ),
                    value=snapshot.json(),
                    # Set expiration time (10 times the submission window * 2 seconds)
                    ex=self._submission_window * 10 * 2,
                )

                # Update Redis with success state
                p = self._redis_conn.pipeline()
                p.hset(
                    name=epoch_id_project_to_state_mapping(
                        epoch_id=msg_obj.epochId, state_id=SnapshotterStates.SNAPSHOT_BUILD.value,
                    ),
                    mapping={
                        project_id: SnapshotterStateUpdate(
                            status='success', timestamp=int(time.time()),
                        ).json(),
                    },
                )
                await p.execute()

                await self._commit_payload(
                    task_type=task_type,
                    project_id=project_id,
                    epoch=msg_obj,
                    snapshot=snapshot,
                    storage_flag=settings.web3storage.upload_snapshots,
                    _ipfs_writer_client=self._ipfs_writer_client,
                )

    async def _process_task(self, msg_obj: PowerloomSnapshotProcessMessage, task_type: str):
        """
        Process a PowerloomSnapshotProcessMessage object for a given task type.

        This method initializes necessary components and delegates the processing
        to either single mode or bulk mode based on the message object.

        Args:
            msg_obj (PowerloomSnapshotProcessMessage): The message object to process.
            task_type (str): The type of task to perform.
        """
        self._logger.debug(
            'Processing callback: {}', msg_obj,
        )
        if task_type not in self._project_calculation_mapping:
            self._logger.error(
                (
                    'No project calculation mapping found for task type'
                    f' {task_type}. Skipping...'
                ),
            )
            return

        # Load rate limiting scripts if not already loaded
        if not self._rate_limiting_lua_scripts:
            self._rate_limiting_lua_scripts = await load_rate_limiter_scripts(
                self._redis_conn,
            )
        self._logger.debug(
            'Got epoch to process for {}: {}',
            task_type, msg_obj,
        )

        # Process in bulk mode or single mode based on the message object
        if msg_obj.bulk_mode:
            await self._process_bulk_mode(msg_obj=msg_obj, task_type=task_type)
        else:
            await self._process_single_mode(msg_obj=msg_obj, task_type=task_type)
        await self._redis_conn.close()

    async def _on_rabbitmq_message(self, message: IncomingMessage):
        """
        Callback function for processing messages received from RabbitMQ.

        This method acknowledges the message, initializes the worker,
        parses the message, and starts the processor task.

        Args:
            message (IncomingMessage): The incoming message from RabbitMQ.
        """
        task_type = message.routing_key.split('.')[-1]
        if task_type not in self._task_types:
            return

        await message.ack()

        await self.init_worker()

        self._logger.debug('task type: {}', task_type)

        try:
            # Parse the message body into a PowerloomSnapshotProcessMessage object
            msg_obj: PowerloomSnapshotProcessMessage = (
                PowerloomSnapshotProcessMessage.parse_raw(message.body)
            )
        except ValidationError as e:
            self._logger.opt(exception=True).error(
                (
                    'Bad message structure of callback processor. Error: {}, {}'
                ),
                e, message.body,
            )
            return
        except Exception as e:
            self._logger.opt(exception=True).error(
                (
                    'Unexpected message structure of callback in processor. Error: {}'
                ),
                e,
            )
            return

        # Start the processor task
        current_time = time.time()
        task = asyncio.create_task(self._process_task(msg_obj=msg_obj, task_type=task_type))
        self._active_tasks.add((current_time, task))
        task.add_done_callback(lambda _: self._active_tasks.discard((current_time, task)))

    async def _init_project_calculation_mapping(self):
        """
        Initialize the project calculation mapping.

        This method creates a dictionary that maps project types to their corresponding
        calculation classes based on the projects configuration.

        Raises:
            Exception: If a duplicate project type is found in the projects configuration.
        """
        if self._project_calculation_mapping is not None:
            return
        # Generate project function mapping
        self._project_calculation_mapping = dict()
        for project_config in projects_config:
            key = project_config.project_type
            if key in self._project_calculation_mapping:
                raise Exception('Duplicate project type found')
            module = importlib.import_module(project_config.processor.module)
            class_ = getattr(module, project_config.processor.class_name)
            self._project_calculation_mapping[key] = class_()

    async def _init_ipfs_client(self):
        """
        Initialize the IPFS client.

        This method creates a singleton instance of AsyncIPFSClientSingleton,
        initializes its sessions, and assigns the write and read clients to instance variables.
        """
        self._ipfs_singleton = AsyncIPFSClientSingleton(settings.ipfs)
        await self._ipfs_singleton.init_sessions()
        self._ipfs_writer_client = self._ipfs_singleton._ipfs_write_client
        self._ipfs_reader_client = self._ipfs_singleton._ipfs_read_client

    async def init_worker(self):
        """
        Initialize the worker.

        This method initializes the project calculation mapping, IPFS client,
        and other necessary components if they haven't been initialized yet.
        """
        if not self._initialized:
            await self._init_project_calculation_mapping()
            await self._init_ipfs_client()
            await self.init()


if __name__ == '__main__':
    snapshot_worker = SnapshotAsyncWorker('SnapshotAsyncWorker')
    snapshot_worker.run()
