from enum import Enum
from typing import List
from typing import Optional
from typing import Union

from ipfs_client.settings.data_models import IPFSConfig
from pydantic import BaseModel
from pydantic import Field


class Auth(BaseModel):
    """Authentication configuration model."""
    enabled: bool = Field(True, description='Whether auth is enabled or not')
    header_key: str = Field('X-API-KEY', description='Key used for auth')


class CoreAPI(BaseModel):
    """Core API configuration model."""
    host: str
    port: int
    auth: Auth
    public_rate_limit: str


class RPCNodeConfig(BaseModel):
    """RPC node configuration model."""
    url: str


class ConnectionLimits(BaseModel):
    """Connection limits configuration model."""
    max_connections: int = 100
    max_keepalive_connections: int = 50
    keepalive_expiry: int = 300


class RateLimitConfig(BaseModel):
    """RPC Rate limit configuration model."""
    requests_per_second: int
    requests_per_minute: int
    requests_per_day: int


class RPCConfigBase(BaseModel):
    """Base RPC configuration model."""
    full_nodes: List[RPCNodeConfig]
    archive_nodes: Optional[List[RPCNodeConfig]]
    force_archive_blocks: Optional[int]
    retry: int
    request_time_out: int
    connection_limits: ConnectionLimits


class RPCConfigFull(RPCConfigBase):
    """Full RPC configuration model."""
    skip_epoch_threshold_blocks: int
    polling_interval: int
    semaphore_value: int = 20
    rate_limit: RateLimitConfig


class RLimit(BaseModel):
    """Resource limit configuration model."""
    file_descriptors: int


class Timeouts(BaseModel):
    """Timeout configuration model."""
    basic: int
    archival: int
    connection_init: int


class QueueConfig(BaseModel):
    """Queue configuration model."""
    num_instances: int


class RabbitMQConfig(BaseModel):
    """RabbitMQ configuration model."""
    exchange: str


class RabbitMQSetup(BaseModel):
    """RabbitMQ setup configuration model."""
    core: RabbitMQConfig
    callbacks: RabbitMQConfig
    event_detector: RabbitMQConfig
    commit_payload: RabbitMQConfig
    delegated_worker: RabbitMQConfig


class RabbitMQ(BaseModel):
    """RabbitMQ connection configuration model."""
    user: str
    password: str
    host: str
    port: int
    setup: RabbitMQSetup


class ReportingConfig(BaseModel):
    """Reporting configuration model."""
    slack_url: str
    service_url: str
    telegram_url: str
    telegram_chat_id: str
    failure_report_frequency: int


class Redis(BaseModel):
    """Redis configuration model."""
    host: str
    port: int
    db: int
    password: Union[str, None] = None
    ssl: bool = False
    cluster_mode: bool = False


class RedisReader(BaseModel):
    """Redis reader configuration model."""
    host: str
    port: int
    db: int
    password: Union[str, None] = None
    ssl: bool = False
    cluster_mode: bool = False


class Logs(BaseModel):
    """Logging configuration model."""
    trace_enabled: bool
    write_to_files: bool


class EventContract(BaseModel):
    """Event contract configuration model."""
    address: str
    abi: str
    deadline_buffer: int
    day_counter_buffer: int


class CallbackWorkerConfig(BaseModel):
    """Callback worker configuration model."""
    num_snapshot_workers: int
    num_aggregation_workers: int
    num_delegate_workers: int


class IPFSWriterRateLimit(BaseModel):
    """IPFS writer rate limit configuration model."""
    req_per_sec: int
    burst: int


class ExternalAPIAuth(BaseModel):
    """External API authentication configuration model."""
    apiKey: str
    apiSecret: str = ''  # This is most likely used as a basic auth tuple of (username, password)


class Web3Storage(BaseModel):
    """Web3 storage configuration model."""
    upload_snapshots: bool
    upload_aggregates: bool
    url: str
    upload_url_suffix: str
    api_token: str
    timeout: int
    max_idle_conns: int
    idle_conn_timeout: int
    # rate_limit: Optional[IPFSWriterRateLimit] = None


class RelayerService(BaseModel):
    """Relayer service configuration model."""
    host: str
    port: str
    keepalive_secs: int


class SignerConfig(BaseModel):
    """Signer configuration model."""
    address: str
    private_key: str


class TxSubmissionConfig(BaseModel):
    """Transaction submission configuration model."""
    enabled: bool = False
    # relayer: RelayerService
    signers: List[SignerConfig] = []


class HTTPXConfig(BaseModel):
    """HTTPX client configuration model."""
    pool_timeout: int
    connect_timeout: int
    read_timeout: int
    write_timeout: int


class Settings(BaseModel):
    """Main settings configuration model."""
    namespace: str
    signer_private_key: str
    core_api: CoreAPI
    instance_id: str
    slot_id: int
    rpc: RPCConfigFull
    local_collector_port: int
    rlimit: RLimit
    httpx: HTTPXConfig
    rabbitmq: RabbitMQ
    reporting: ReportingConfig
    redis: Redis
    redis_reader: RedisReader
    logs: Logs
    data_market: str
    projects_config_path: str
    preloader_config_path: str
    aggregator_config_path: str
    protocol_state: EventContract
    callback_worker_config: CallbackWorkerConfig
    ipfs: IPFSConfig
    web3storage: Web3Storage
    node_version: str
    anchor_chain_rpc: RPCConfigBase


# Projects related models

class ProcessorConfig(BaseModel):
    """Processor configuration model."""
    module: str
    class_name: str


class ProjectConfig(BaseModel):
    """Project configuration model."""
    project_type: str
    projects: Optional[List[str]] = None
    processor: ProcessorConfig
    preload_tasks: List[str]
    bulk_mode: Optional[bool] = False


class ProjectsConfig(BaseModel):
    """Projects configuration model."""
    config: List[ProjectConfig]


class AggregateFilterConfig(BaseModel):
    """Aggregate filter configuration model."""
    projectId: str


class AggregateOn(str, Enum):
    """Enumeration for aggregation types."""
    single_project = 'SingleProject'
    multi_project = 'MultiProject'


class AggregationConfig(BaseModel):
    """Aggregation configuration model."""
    project_type: str
    aggregate_on: AggregateOn
    filters: Optional[AggregateFilterConfig]
    projects_to_wait_for: Optional[List[str]]
    processor: ProcessorConfig


class AggregatorConfig(BaseModel):
    """Aggregator configuration model."""
    config: List[AggregationConfig]


class Preloader(BaseModel):
    """Preloader configuration model."""
    task_type: str
    module: str
    class_name: str


class DelegatedTask(BaseModel):
    """Delegated task configuration model."""
    task_type: str
    module: str
    class_name: str


class PreloaderConfig(BaseModel):
    """Preloader configuration model."""
    preloaders: List[Preloader]
    delegate_tasks: List[DelegatedTask]
    timeout: int
