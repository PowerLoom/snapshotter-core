import asyncio
import functools
from abc import ABC
from abc import ABCMeta
from abc import abstractmethod
from typing import Union
from urllib.parse import urljoin

from httpx import AsyncClient
from httpx import Client as SyncClient
from ipfs_client.main import AsyncIPFSClient
from pydantic import BaseModel
from redis import asyncio as aioredis

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.models.data_models import SnapshotterIssue
from snapshotter.utils.models.data_models import TelegramMessage
from snapshotter.utils.models.data_models import TelegramEpochProcessingReportMessage
from snapshotter.utils.models.data_models import TelegramSnapshotterReportMessage
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.models.message_models import PowerloomSnapshotSubmittedMessage
from snapshotter.utils.redis.redis_keys import callback_last_sent_by_issue
from snapshotter.utils.rpc import RpcHelper

# Setup logger for this module
helper_logger = default_logger.bind(module='Callback|Helpers')


def misc_notification_callback_result_handler(fut: asyncio.Future):
    """
    Handles the result of a callback or notification.

    Args:
        fut (asyncio.Future): The future object representing the callback or notification.

    Returns:
        None
    """
    try:
        r = fut.result()
    except Exception as e:
        # Log the exception with full traceback if debug_mode is True
        if settings.logs.debug_mode:
            helper_logger.opt(exception=settings.logs.debug_mode).error(
                'Exception while sending callback or notification: {}', e,
            )
        else:
            helper_logger.error('Exception while sending callback or notification: {}', e)
    else:
        helper_logger.debug('Callback or notification result:{}', r)


def sync_notification_callback_result_handler(f: functools.partial):
    """
    Handles the result of a synchronous notification callback.

    Args:
        f (functools.partial): The function to handle.

    Returns:
        None
    """
    try:
        result = f()
    except Exception as exc:
        # Log the exception with full traceback if debug_mode is True
        if settings.logs.debug_mode:
            helper_logger.opt(exception=settings.logs.debug_mode).error(
                'Exception while sending callback or notification: {}', exc,
            )
        else:
            helper_logger.error('Exception while sending callback or notification: {}', exc)
    else:
        helper_logger.debug('Callback or notification result:{}', result)


async def send_failure_notifications_async(
    client: AsyncClient,
    message: SnapshotterIssue,
    redis_conn: aioredis.Redis,
):
    """
    Sends failure notifications asynchronously to the configured reporting services.

    Args:
        client (AsyncClient): The async HTTP client to use for sending notifications.
        message (SnapshotterIssue): The message to send as notification.
        redis_conn (aioredis.Redis): Redis connection for rate limiting.

    Returns:
        None
    """
    # Check if the last notification was sent within the minimum reporting interval
    caching_task = []
    if settings.reporting.min_reporting_interval > 0:
        last_sent_timestamp = await redis_conn.get(
            callback_last_sent_by_issue(message.issueType),
        )
        if not last_sent_timestamp:
            caching_task.append(
                redis_conn.set(
                    name=callback_last_sent_by_issue(message.issueType),
                    value=message.timeOfReporting,
                    ex=settings.reporting.min_reporting_interval,
                ),
            )
        else:
            helper_logger.debug(
                'Not sending failure notification for {} because the last notification was sent within the minimum reporting interval',
                message.issueType,
            )
            return

    notification_tasks = []
    if settings.reporting.service_url:
        f = asyncio.create_task(
            client.post(
                url=urljoin(settings.reporting.service_url, '/reportIssue'),
                json=message.dict(),
            ),
        )
        f.add_done_callback(misc_notification_callback_result_handler)
        notification_tasks.append(f)
    if settings.reporting.slack_url:
        f = asyncio.create_task(
            client.post(
                url=settings.reporting.slack_url,
                json=message.dict(),
            ),
        )
        f.add_done_callback(misc_notification_callback_result_handler)
        notification_tasks.append(f)

    if notification_tasks:
        notification_tasks = caching_task + notification_tasks
        await asyncio.gather(*notification_tasks)


def send_failure_notifications_sync(
    client: SyncClient,
    message: SnapshotterIssue,
    redis_conn: aioredis.Redis,
):
    """
    Sends failure notifications synchronously to the reporting service, Slack, and Telegram.

    Args:
        client (SyncClient): The HTTP client to use for sending notifications.
        message (SnapshotterIssue): The message to send as notification.

    Returns:
        None
    """
    # Send notification to the reporting service if configured
    if settings.reporting.service_url:
        f = functools.partial(
            client.post,
            url=urljoin(settings.reporting.service_url, '/reportIssue'),
            json=message.dict(),
        )
        sync_notification_callback_result_handler(f)

    # Send notification to Slack if configured
    if settings.reporting.slack_url:
        f = functools.partial(
            client.post,
            url=settings.reporting.slack_url,
            json=message.dict(),
        )
        sync_notification_callback_result_handler(f)

    # Send notification to Telegram if configured
    if settings.reporting.telegram_url and settings.reporting.telegram_chat_id:
        reporting_message = TelegramEpochProcessingReportMessage(
            chatId=settings.reporting.telegram_chat_id,
            slotId=settings.slot_id,
            issue=message,
        )

        f = functools.partial(
            client.post,
            url=urljoin(settings.reporting.telegram_url, '/reportEpochProcessingIssue'),
            json=reporting_message.dict(),
        )
        sync_notification_callback_result_handler(f)


async def send_telegram_notification_async(client: AsyncClient, message: TelegramMessage):
    """
    Sends an asynchronous Telegram notification for reporting issues.

    This function checks if Telegram reporting is configured, and then sends the appropriate
    message based on its type (epoch processing issue or snapshotter issue).

    Args:
        client (AsyncClient): The async HTTP client to use for sending notifications.
        message (TelegramMessage): The message to send as a Telegram notification.

    Returns:
        None
    """

    if not settings.reporting.telegram_url or not settings.reporting.telegram_chat_id:
        return

    if isinstance(message, TelegramEpochProcessingReportMessage):
        endpoint = '/reportEpochProcessingIssue'
    elif isinstance(message, TelegramSnapshotterReportMessage):
        endpoint = '/reportSnapshotIssue'
    else:
        helper_logger.error(
            f'Unsupported telegram message type: {type(message)} - message not sent',
        )
        return

    f = asyncio.ensure_future(
        client.post(
            url=urljoin(settings.reporting.telegram_url, endpoint),
            json=message.dict(),
        ),
    )
    f.add_done_callback(misc_notification_callback_result_handler)


def send_telegram_notification_sync(client: SyncClient, message: TelegramMessage):
    """
    Sends a synchronous Telegram notification for reporting issues.

    This function checks if Telegram reporting is configured, and then sends the appropriate
    message based on its type (epoch processing issue or snapshotter issue).

    Args:
        client (SyncClient): The synchronous HTTP client to use for sending notifications.
        message (TelegramMessage): The message to send as a Telegram notification.

    Returns:
        None
    """

    if not settings.reporting.telegram_url or not settings.reporting.telegram_chat_id:
        return

    if isinstance(message, TelegramEpochProcessingReportMessage):
        endpoint = '/reportEpochProcessingIssue'
    elif isinstance(message, TelegramSnapshotterReportMessage):
        endpoint = '/reportSnapshotIssue'
    else:
        helper_logger.error(
            f'Unsupported telegram message type: {type(message)} - message not sent',
        )
        return

    f = functools.partial(
        client.post,
        url=urljoin(settings.reporting.telegram_url, endpoint),
        json=message.dict(),
    )
    sync_notification_callback_result_handler(f)


class GenericProcessorSnapshot(ABC):
    """
    Abstract base class for snapshot processors.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
    ):
        """
        Abstract method to compute the snapshot.

        Args:
            epoch (PowerloomSnapshotProcessMessage): The epoch message.
            redis (aioredis.Redis): Redis connection.
            rpc_helper (RpcHelper): RPC helper instance.
        """
        pass


class GenericPreloader(ABC):
    """
    Abstract base class for preloaders.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    async def compute(
        self,
        epoch: EpochBase,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
    ):
        """
        Abstract method to compute preload data.

        Args:
            epoch (EpochBase): The epoch message.
            redis_conn (aioredis.Redis): Redis connection.
            rpc_helper (RpcHelper): RPC helper instance.
        """
        pass

    @abstractmethod
    async def cleanup(self):
        """
        Abstract method to clean up resources.
        """
        pass


class GenericProcessorAggregate(ABC):
    """
    Abstract base class for aggregate processors.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    async def compute(
        self,
        msg_obj: Union[PowerloomSnapshotSubmittedMessage, PowerloomCalculateAggregateMessage],
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        project_id: str,
    ):
        """
        Abstract method to compute aggregate processing.

        Args:
            msg_obj (Union[PowerloomSnapshotSubmittedMessage, PowerloomCalculateAggregateMessage]): The message object.
            redis (aioredis.Redis): Redis connection.
            rpc_helper (RpcHelper): RPC helper instance.
            anchor_rpc_helper (RpcHelper): Anchor RPC helper instance.
            ipfs_reader (AsyncIPFSClient): IPFS reader instance.
            protocol_state_contract: Protocol state contract.
            project_id (str): Project ID.
        """
        pass


class PreloaderAsyncFutureDetails(BaseModel):
    """
    Pydantic model for preloader async future details.
    """
    obj: GenericPreloader
    future: asyncio.Task

    class Config:
        arbitrary_types_allowed = True
