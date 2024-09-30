import web3.datastructures
from pydantic import ValidationError
from redis import asyncio as aioredis

from snapshotter.utils.callback_helpers import GenericDelegateProcessor
from snapshotter.utils.default_logger import logger
from snapshotter.utils.helper_functions import attribute_dict_to_dict
from snapshotter.utils.models.message_models import PowerloomDelegateTxReceiptWorkerResponseMessage
from snapshotter.utils.models.message_models import PowerloomDelegateWorkerRequestMessage
from snapshotter.utils.rpc import RpcHelper


class TxReceiptProcessor(GenericDelegateProcessor):
    """
    A processor class for handling transaction receipts.

    This class extends GenericDelegateProcessor and provides functionality
    to process and fetch transaction receipts.
    """

    def __init__(self) -> None:
        """
        Initialize the TxReceiptProcessor.

        Sets up a logger instance for this processor.
        """
        self._logger = logger.bind(module='TxReceiptPreloader')

    async def compute(
            self,
            msg_obj: PowerloomDelegateWorkerRequestMessage,
            redis_conn: aioredis.Redis,
            rpc_helper: RpcHelper,
    ):
        """
        Process the received message object to fetch and return transaction receipt.

        Args:
            msg_obj (PowerloomDelegateWorkerRequestMessage): The message object containing request details.
            redis_conn (aioredis.Redis): Redis connection for caching.
            rpc_helper (RpcHelper): Helper object for making RPC calls.

        Returns:
            PowerloomDelegateTxReceiptWorkerResponseMessage: A response message containing the transaction receipt.

        Raises:
            ValidationError: If no transaction hash is found in the message object.
        """
        self._logger.trace(
            'Processing tx receipt fetch for {}', msg_obj,
        )

        # Validate the presence of tx_hash in the message object
        if 'tx_hash' not in msg_obj.extra:
            raise ValidationError(
                f'No tx_hash found in'
                f' {msg_obj.extra}. Skipping...',
            )

        # Extract the transaction hash
        tx_hash = msg_obj.extra['tx_hash']

        # Fetch the transaction receipt
        tx_receipt_obj: web3.datastructures.AttributeDict = await rpc_helper.get_transaction_receipt(
            tx_hash,
        )

        # Convert AttributeDict to regular dictionary
        tx_receipt_dict = attribute_dict_to_dict(tx_receipt_obj)

        # Construct and return the response message
        return PowerloomDelegateTxReceiptWorkerResponseMessage(
            txHash=tx_hash,
            requestId=msg_obj.requestId,
            txReceipt=tx_receipt_dict,
            epochId=msg_obj.epochId,
        )
