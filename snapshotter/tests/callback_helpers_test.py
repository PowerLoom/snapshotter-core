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
