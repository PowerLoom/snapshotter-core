from redis import asyncio as aioredis

from snapshotter.utils.callback_helpers import GenericPreloader
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import get_block_details_in_block_range


class BlockDetailsPreloader(GenericPreloader):
    """
    A preloader class for fetching and caching block details within a specified epoch range.
    
    This class extends GenericPreloader and implements methods to compute and cleanup
    block details for a given epoch range.
    """

    def __init__(self) -> None:
        """
        Initialize the BlockDetailsPreloader with a logger.
        """
        self._logger = logger.bind(module='BlockDetailsPreloader')

    async def compute(
            self,
            epoch: EpochBase,
            redis_conn: aioredis.Redis,
            rpc_helper: RpcHelper,
    ):
        """
        Compute and cache block details for the given epoch range.

        Args:
            epoch (EpochBase): The epoch object containing begin and end block heights.
            redis_conn (aioredis.Redis): Redis connection for caching.
            rpc_helper (RpcHelper): Helper object for making RPC calls.

        This method fetches block details for all blocks within the epoch range
        and caches them in Redis.
        """
        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        try:
            # Fetch and cache block details for all blocks in the specified range
            await get_block_details_in_block_range(
                from_block=min_chain_height,
                to_block=max_chain_height,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
        except Exception as e:
            # Log any errors that occur during the process
            self._logger.error(f'Error in block details preloader: {e}')
        finally:
            # Ensure Redis connection is closed after operation
            await redis_conn.close()

    async def cleanup(self):
        """
        Perform any necessary cleanup operations.

        This method is currently a placeholder and does not perform any actions.
        It can be implemented in the future if cleanup operations are needed.
        """
        pass
