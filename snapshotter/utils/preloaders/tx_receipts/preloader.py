import json

from pydantic import ValidationError
from redis import asyncio as aioredis

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.generic_delegator_preloader import DelegatorPreloaderAsyncWorker
from snapshotter.utils.helper_functions import preloading_entry_exit_logger
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.models.message_models import PowerloomDelegateTxReceiptWorkerResponseMessage
from snapshotter.utils.models.message_models import PowerloomDelegateWorkerRequestMessage
from snapshotter.utils.redis.redis_keys import epoch_txs_htable
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.snapshot_utils import get_block_details_in_block_range


class TxPreloadWorker(DelegatorPreloaderAsyncWorker):
    """
    A worker class for preloading transaction receipts.
    Inherits from DelegatorPreloaderAsyncWorker.
    """

    def __init__(self) -> None:
        """
        Initialize the TxPreloadWorker.
        Sets the task type to 'txreceipt'.
        """
        super(TxPreloadWorker, self).__init__()
        self._task_type = 'txreceipt'

    async def _handle_filter_worker_response_message(self, message: bytes):
        """
        Handle the response message from the filter worker.

        Args:
            message (bytes): The raw message received from the worker.

        This method parses the message, validates it, and processes the transaction receipt.
        """
        try:
            msg_obj: PowerloomDelegateTxReceiptWorkerResponseMessage = (
                PowerloomDelegateTxReceiptWorkerResponseMessage.parse_raw(message)
            )
        except ValidationError:
            self._logger.opt(exception=settings.logs.trace_enabled).error(
                'Bad message structure of txreceiptResponse',
            )
            return
        except Exception:
            self._logger.opt(exception=True).error(
                'Unexpected message format of txreceiptResponse',
            )
            return

        if msg_obj.txReceipt is None:
            self._logger.warning(
                'Received txreceiptResponse with empty txReceipt for requestId'
                f' {msg_obj.requestId} for epoch {msg_obj.epochId}',
            )
            return

        async with self._rw_lock.reader_lock:
            if msg_obj.requestId not in self._awaited_delegated_response_ids:
                # Uncomment for debugging purposes
                # self._logger.warning(
                #     f'Received txreceiptResponse for unknown requestId {msg_obj.requestId} for epoch {msg_obj.epochId}',
                # )
                # self._logger.warning(
                #     'Known requestIds for epoch '
                #     f'{msg_obj.epochId}: {self._awaited_delegated_response_ids}',
                # )
                return

        async with self._rw_lock.writer_lock:
            self._awaited_delegated_response_ids.remove(msg_obj.requestId)
            self._collected_response_objects.update(
                {msg_obj.txHash: msg_obj.txReceipt},
            )

    async def _on_delegated_responses_complete(self):
        """
        Handle the completion of all delegated responses.
        This method is called when all expected responses have been received.
        It stores the collected transaction receipts in Redis.
        """
        if self._collected_response_objects:
            await self._redis_conn.hset(
                name=epoch_txs_htable(epoch_id=self._epoch.epochId),
                mapping={
                    k: json.dumps(v)
                    for k, v in self._collected_response_objects.items()
                },
            )

    @preloading_entry_exit_logger
    async def compute(self, epoch: EpochBase, redis_conn: aioredis.Redis, rpc_helper: RpcHelper):
        """
        Compute method to preload transaction receipts for a given epoch.

        Args:
            epoch (EpochBase): The epoch for which to preload transaction receipts.
            redis_conn (aioredis.Redis): Redis connection object.
            rpc_helper (RpcHelper): RPC helper object for blockchain interactions.

        This method performs the following steps:
        1. Set up logging and store epoch and Redis connection.
        2. Clean up old data from Redis.
        3. Fetch block details for the epoch's block range.
        4. Extract transactions from the blocks.
        5. Create worker request messages for each transaction.
        6. Delegate the computation to the parent class method.
        """
        self._logger = default_logger.bind(module='TxPreloadWorker')
        self._epoch = epoch
        self._redis_conn = redis_conn

        # Clean up hset for current epoch - 30 if it exists
        await self._redis_conn.delete(epoch_txs_htable(epoch_id=self._epoch.epochId - 30))

        tx_list = list()
        block_details = await get_block_details_in_block_range(
            from_block=epoch.begin,
            to_block=epoch.end,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )
        [tx_list.extend(block['transactions']) for block in block_details.values()]

        # Create worker request messages for each transaction
        tx_receipt_query_messages = [
            PowerloomDelegateWorkerRequestMessage(
                epochId=epoch.epochId,
                extra={'tx_hash': tx_hash},
                requestId=idx + 1,
                task_type=self._task_type,
            )
            for idx, tx_hash in enumerate(tx_list)
        ]
        self._request_id_query_obj_map = {
            msg_obj.requestId: msg_obj
            for msg_obj in tx_receipt_query_messages
        }

        # Delegate the computation to the parent class method
        return await super(TxPreloadWorker, self).compute_with_retry(epoch, redis_conn, rpc_helper)
