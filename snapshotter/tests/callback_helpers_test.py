import time
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fakeredis import FakeAsyncRedis
from pytest_asyncio import fixture as async_fixture

from snapshotter.settings.config import settings
from snapshotter.utils.callback_helpers import send_failure_notifications_async
from snapshotter.utils.callback_helpers import send_failure_notifications_sync
from snapshotter.utils.models.data_models import SnapshotterIssue
from snapshotter.utils.redis.redis_keys import callback_last_sent_by_issue


@async_fixture(scope='module')
async def mock_redis():
    """Fixture to provide a FakeAsyncRedis connection."""
    fake_redis = FakeAsyncRedis()
    yield fake_redis
    await fake_redis.close()


@async_fixture(scope='module')
async def mock_async_client():
    """Fixture to provide a mocked AsyncClient."""
    with patch('snapshotter.utils.callback_helpers.AsyncClient', autospec=True) as MockClient:
        mock_client_instance = MockClient.return_value
        mock_client_instance.post = AsyncMock()
        yield mock_client_instance


@async_fixture(scope='module')
async def mock_sync_client():
    """Fixture to provide a mocked SyncClient."""
    with patch('snapshotter.utils.callback_helpers.SyncClient', autospec=True) as MockClient:
        mock_client_instance = MockClient.return_value
        mock_client_instance.post = MagicMock()
        yield mock_client_instance


@pytest.mark.asyncio(loop_scope='module')
async def test_send_failure_notifications_async_with_service_and_slack_urls(mock_async_client, mock_redis):
    """Test sending failure notifications when both service_url and slack_url are set."""
    with patch('snapshotter.settings.config.settings.reporting.service_url', 'https://mock-service-url'), \
            patch('snapshotter.settings.config.settings.reporting.slack_url', 'https://mock-slack-url'):

        message = SnapshotterIssue(
            instanceID='test_instance',
            issueType='TEST_ISSUE',
            projectID='test_project',
            epochId=123,
            timeOfReporting=int(time.time()),
            extra='Test extra info',
        )
        await send_failure_notifications_async(
            client=mock_async_client,
            message=message,
            redis_conn=mock_redis,
        )

        # Assert that both service and slack notifications were sent
        assert mock_async_client.post.call_count == 2

        # Verify that the current timestamp is set in Redis
        last_sent = await mock_redis.get(callback_last_sent_by_issue(message.issueType))
        assert int(last_sent) == int(message.timeOfReporting), f'Last sent timestamp does not match.'

        # Clean up
        await mock_redis.flushall()
        mock_async_client.post.reset_mock()


@pytest.mark.asyncio(loop_scope='module')
async def test_send_failure_notifications_async_with_min_reporting_interval(mock_async_client, mock_redis):
    """Test that notifications are not sent again within the min_reporting_interval."""
    with patch('snapshotter.settings.config.settings.reporting.min_reporting_interval', 5), \
            patch('snapshotter.settings.config.settings.reporting.service_url', 'https://mock-service-url'), \
            patch('snapshotter.settings.config.settings.reporting.slack_url', 'https://mock-slack-url'):

        message = SnapshotterIssue(
            instanceID='test_instance',
            issueType='TEST_ISSUE_INTERVAL',
            projectID='test_project',
            epochId=124,
            timeOfReporting=int(time.time()),
            extra='Test extra info for interval',
        )

        # Simulate that a notification was sent recently
        await mock_redis.set(
            callback_last_sent_by_issue(message.issueType),
            message.timeOfReporting,
            ex=settings.reporting.min_reporting_interval,
        )

        await send_failure_notifications_async(
            client=mock_async_client,
            message=message,
            redis_conn=mock_redis,
        )

        # No new notifications should be sent
        assert mock_async_client.post.call_count == 0

        # Clean up
        await mock_redis.flushall()
        mock_async_client.post.reset_mock()


@pytest.mark.asyncio(loop_scope='module')
async def test_send_failure_notifications_async_without_service_and_slack_urls(mock_async_client, mock_redis):
    """Test that no notifications are sent when service_url and slack_url are not set."""
    with patch('snapshotter.settings.config.settings.reporting.service_url', ''), \
            patch('snapshotter.settings.config.settings.reporting.slack_url', ''):

        message = SnapshotterIssue(
            instanceID='test_instance',
            issueType='TEST_ISSUE_NO_URLS',
            projectID='test_project',
            epochId=125,
            timeOfReporting='2023-10-01T00:00:00Z',
            extra='Test extra info with no URLs',
        )

        await send_failure_notifications_async(
            client=mock_async_client,
            message=message,
            redis_conn=mock_redis,
        )

        # No notifications should be sent
        assert mock_async_client.post.call_count == 0

        last_sent = await mock_redis.get(callback_last_sent_by_issue(message.issueType))
        assert last_sent is None

        # Clean up
        await mock_redis.flushall()
        mock_async_client.post.reset_mock()


@pytest.mark.asyncio(loop_scope='module')
async def test_send_failure_notifications_async_min_interval_expired(mock_async_client, mock_redis):
    """Test that notifications are sent again after min_reporting_interval has expired."""
    # Setup settings
    with patch('snapshotter.settings.config.settings.reporting.min_reporting_interval', 1), \
            patch('snapshotter.settings.config.settings.reporting.service_url', 'https://mock-service-url/reportIssue'), \
            patch('snapshotter.settings.config.settings.reporting.slack_url', 'https://mock-slack-url'):

        # Create a sample message
        message = SnapshotterIssue(
            instanceID='test_instance',
            issueType='TEST_ISSUE_MIN_INTERVAL',
            projectID='test_project',
            epochId=127,
            timeOfReporting=int(time.time()),
            extra='Test extra info for min interval',
        )

        # Simulate that a notification was sent 6 seconds ago
        await mock_redis.set(
            callback_last_sent_by_issue(message.issueType),
            message.timeOfReporting,
            ex=settings.reporting.min_reporting_interval,
        )

        time.sleep(settings.reporting.min_reporting_interval)

        await send_failure_notifications_async(
            client=mock_async_client,
            message=message,
            redis_conn=mock_redis,
        )

        # Notifications should be sent again
        assert mock_async_client.post.call_count == 2

        # Verify that the timestamp was updated in Redis
        last_sent = await mock_redis.get(callback_last_sent_by_issue(message.issueType))
        assert int(last_sent) == int(message.timeOfReporting), f'Last sent timestamp does not match.'

        # Clean up
        await mock_redis.flushall()
        mock_async_client.post.reset_mock()


@pytest.mark.asyncio(loop_scope='module')
async def test_send_failure_notifications_sync_with_service_and_slack_urls(mock_sync_client):
    """Test sending failure notifications synchronously when both service_url and slack_url are set."""
    # Setup settings
    with patch('snapshotter.settings.config.settings.reporting.service_url', 'https://mock-service-url'), \
            patch('snapshotter.settings.config.settings.reporting.slack_url', 'https://mock-slack-url'):

        # Create a sample message
        message = SnapshotterIssue(
            instanceID='test_instance',
            issueType='TEST_SYNC_ISSUE',
            projectID='test_project',
            epochId=128,
            timeOfReporting=int(time.time()),
            extra='Test extra info for sync',
        )

        # Invoke the sync function
        send_failure_notifications_sync(
            client=mock_sync_client,
            message=message,
            redis_conn=mock_redis,
        )

        assert mock_sync_client.post.call_count == 2

        # Clean up
        mock_sync_client.post.reset_mock()


def test_send_failure_notifications_sync_without_service_and_slack_urls(mock_sync_client):
    """Test that no notifications are sent synchronously when service_url and slack_url are not set."""
    # Setup settings
    with patch('snapshotter.settings.config.settings.reporting.service_url', ''), \
            patch('snapshotter.settings.config.settings.reporting.slack_url', ''):

        # Create a sample message
        message = SnapshotterIssue(
            instanceID='test_instance',
            issueType='TEST_SYNC_NO_URLS',
            projectID='test_project',
            epochId=129,
            timeOfReporting=int(time.time()),
            extra='Test extra info with no URLs for sync',
        )

        # Invoke the sync function
        send_failure_notifications_sync(
            client=mock_sync_client,
            message=message,
            redis_conn=mock_redis,
        )

        # No notifications should be sent
        mock_sync_client.post.assert_not_called()

        # Clean up
        mock_sync_client.post.reset_mock()
