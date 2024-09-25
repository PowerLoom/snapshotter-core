import redis
from redis import asyncio as aioredis
from redis.asyncio.connection import ConnectionPool

from snapshotter.auth.conf import auth_settings


def construct_redis_url():
    """
    Construct a Redis URL based on the authentication settings.

    Returns:
        str: A Redis URL string with or without password, depending on the configuration.
    """
    if auth_settings.redis.password:
        return (
            f'redis://{auth_settings.redis.password}@{auth_settings.redis.host}:{auth_settings.redis.port}'
            f'/{auth_settings.redis.db}'
        )
    else:
        return f'redis://{auth_settings.redis.host}:{auth_settings.redis.port}/{auth_settings.redis.db}'

# Reference: https://github.com/redis/redis-py/issues/936
# This issue discusses handling ReadOnlyError in Redis clusters


async def get_aioredis_pool(pool_size=200):
    """
    Create and return an asynchronous Redis connection pool.

    Args:
        pool_size (int): The maximum number of connections in the pool. Defaults to 200.

    Returns:
        aioredis.Redis: An asynchronous Redis client instance with a connection pool.
    """
    pool = ConnectionPool.from_url(
        url=construct_redis_url(),
        retry_on_error=[redis.exceptions.ReadOnlyError],
        max_connections=pool_size,
    )

    return aioredis.Redis(connection_pool=pool)


class RedisPoolCache:
    """
    A class to manage a Redis connection pool cache.
    """

    def __init__(self, pool_size=500):
        """
        Initialize the RedisPoolCache.

        Args:
            pool_size (int): The maximum number of connections in the pool. Defaults to 500.
        """
        self._aioredis_pool = None
        self._pool_size = pool_size

    async def populate(self):
        """
        Populate the Redis pool cache if it hasn't been initialized.

        This method is asynchronous and should be awaited.
        """
        if not self._aioredis_pool:
            self._aioredis_pool: aioredis.Redis = await get_aioredis_pool(
                self._pool_size,
            )
