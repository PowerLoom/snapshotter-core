from redis import asyncio as aioredis

from snapshotter.utils.callback_helpers import GenericPreloader
from snapshotter.utils.default_logger import logger
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import get_eth_price_usd


class EthPricePreloader(GenericPreloader):
    """
    A preloader class for fetching Ethereum prices for a range of blocks.
    
    This class extends GenericPreloader and implements methods to compute
    and store Ethereum prices for a given epoch range.
    """

    def __init__(self) -> None:
        """
        Initialize the EthPricePreloader with a logger.
        """
        self._logger = logger.bind(module='BlockDetailsPreloader')

    async def compute(
            self,
            epoch: EpochBase,
            redis_conn: aioredis.Redis,
            rpc_helper: RpcHelper,
    ):
        """
        Compute and store Ethereum prices for the given epoch range.

        Args:
            epoch (EpochBase): The epoch containing the block range.
            redis_conn (aioredis.Redis): Redis connection for caching.
            rpc_helper (RpcHelper): Helper for making RPC calls.

        Returns:
            None
        """
        min_chain_height = epoch.begin
        max_chain_height = epoch.end

        try:
            # Fetch Ethereum prices for all blocks in the specified range
            await get_eth_price_usd(
                from_block=min_chain_height,
                to_block=max_chain_height,
                redis_conn=redis_conn,
                rpc_helper=rpc_helper,
            )
        except Exception as e:
            # Log any errors that occur during price fetching
            self._logger.error(f'Error in Eth Price preloader: {e}')
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
