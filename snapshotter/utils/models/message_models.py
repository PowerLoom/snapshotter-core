from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from pydantic import BaseModel
from pydantic import Field


class TxLogsModel(BaseModel):
    """Model representing transaction logs."""
    logIndex: str
    blockNumber: str
    blockHash: str
    transactionHash: str
    transactionIndex: str
    address: str
    data: str
    topics: List[str]


class EthTransactionReceipt(BaseModel):
    """Model representing an Ethereum transaction receipt."""
    transactionHash: str
    transactionIndex: str
    blockHash: str
    blockNumber: str
    from_field: str = Field(..., alias='from')  # 'from' is a reserved keyword in Python
    to: Optional[str]
    cumulativeGasUsed: str
    gasUsed: str
    effectiveGasPrice: str
    logs: List[TxLogsModel]
    contractAddress: Optional[str] = None
    logsBloom: str
    status: str
    type: Optional[str]
    root: Optional[str]


class EpochBase(BaseModel):
    """Base model for epoch-related data."""
    epochId: int
    begin: int
    end: int


class PowerloomSnapshotProcessMessage(EpochBase):
    """Model for Powerloom snapshot process messages."""
    data_source: Optional[str] = None
    primary_data_source: Optional[str] = None
    bulk_mode: Optional[bool] = False


class PowerloomSnapshotFinalizedMessage(BaseModel):
    """Model for Powerloom snapshot finalized messages."""
    epochId: int
    epochEnd: int
    projectId: str
    snapshotCid: str
    timestamp: int


class PowerloomSnapshotBatchSubmittedMessage(BaseModel):
    """Model for Powerloom snapshot batch submitted messages."""
    epochId: int
    batchId: int
    timestamp: int
    transactionHash: str


class PowerloomSnapshotSubmittedMessage(BaseModel):
    """Model for Powerloom snapshot submission messages."""
    snapshotCid: str
    epochId: int
    projectId: str
    timestamp: int


class PowerloomDelegateWorkerRequestMessage(BaseModel):
    """Model for Powerloom delegate worker request messages."""
    epochId: int
    requestId: int
    task_type: str
    extra: Optional[Dict[Any, Any]] = dict()


class PowerloomDelegateWorkerResponseMessage(BaseModel):
    """Model for Powerloom delegate worker response messages."""
    epochId: int
    requestId: int


class PowerloomDelegateTxReceiptWorkerResponseMessage(PowerloomDelegateWorkerResponseMessage):
    """Model for Powerloom delegate transaction receipt worker response messages."""
    txHash: str
    txReceipt: Dict[Any, Any]


class PowerloomCalculateAggregateMessage(BaseModel):
    """Model for Powerloom calculate aggregate messages."""
    messages: List[PowerloomSnapshotSubmittedMessage]
    epochId: int
    timestamp: int


class AggregateBase(BaseModel):
    """Base model for aggregate-related data."""
    epochId: int


class PayloadCommitMessage(BaseModel):
    """Model for payload commit messages."""
    sourceChainId: int
    projectId: str
    epochId: int
    snapshotCID: str


class PayloadCommitFinalizedMessage(BaseModel):
    """Model for payload commit finalized messages."""
    message: PowerloomSnapshotFinalizedMessage
    sourceChainId: int
