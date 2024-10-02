import pika

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger

# Setup logging for RabbitMQ initialization
init_rmq_logger = default_logger.bind(module='RabbitMQ|Init')


def create_rabbitmq_conn() -> pika.BlockingConnection:
    """
    Creates a connection to RabbitMQ using the settings specified in the application configuration.

    Returns:
        pika.BlockingConnection: A blocking connection object representing the connection to RabbitMQ.
    """
    c = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=settings.rabbitmq.host,
            port=settings.rabbitmq.port,
            virtual_host='/',
            credentials=pika.PlainCredentials(
                username=settings.rabbitmq.user,
                password=settings.rabbitmq.password,
            ),
            heartbeat=30,
        ),
    )
    return c


def get_snapshot_queue_routing_key_pattern() -> tuple[str, str]:
    """
    Returns the queue name and routing key pattern for snapshot messages.

    Returns:
        tuple[str, str]: A tuple containing the queue name and routing key pattern.
    """
    queue_name = f'powerloom-backend-cb-snapshot:{settings.namespace}:{settings.instance_id}'
    routing_key_pattern = f'powerloom-backend-callback:{settings.namespace}:{settings.instance_id}:EpochReleased.*'
    return queue_name, routing_key_pattern


def get_aggregate_queue_routing_key_pattern() -> tuple[str, str]:
    """
    Returns the queue name and routing key pattern for the aggregate queue.

    Returns:
        tuple[str, str]: A tuple containing the queue name and routing key pattern.
    """
    queue_name = f'powerloom-backend-cb-aggregate:{settings.namespace}:{settings.instance_id}'
    routing_key_pattern = f'powerloom-backend-callback:{settings.namespace}:{settings.instance_id}:CalculateAggregate.*'
    return queue_name, routing_key_pattern


def get_delegate_worker_request_queue_routing_key() -> tuple[str, str]:
    """
    Returns the name and routing key for the request queue used by the delegated worker.

    Returns:
        tuple[str, str]: A tuple containing the request queue name and routing key.
    """
    request_queue_routing_key = f'powerloom-delegated-worker:{settings.namespace}:{settings.instance_id}:Request'
    request_queue_name = f'powerloom-delegated-worker-request:{settings.namespace}:{settings.instance_id}'
    return request_queue_name, request_queue_routing_key


def get_delegate_worker_response_queue_routing_key_pattern() -> tuple[str, str]:
    """
    Returns the response queue name and routing key pattern for a delegated worker.

    Returns:
        tuple[str, str]: A tuple containing the response queue name and routing key pattern.
    """
    response_queue_routing_key = f'powerloom-delegated-worker:{settings.namespace}:{settings.instance_id}:Response.*'
    response_queue_name = f'powerloom-delegated-worker-response:{settings.namespace}:{settings.instance_id}'
    return response_queue_name, response_queue_routing_key


def init_queue(
    ch: pika.adapters.blocking_connection.BlockingChannel,
    queue_name: str,
    routing_key: str,
    exchange_name: str,
    bind: bool = True,
) -> None:
    """
    Declare a queue and optionally bind it to an exchange with a routing key.

    Args:
        ch (pika.adapters.blocking_connection.BlockingChannel): A blocking channel object from a Pika connection.
        queue_name (str): The name of the queue to declare.
        routing_key (str): The routing key to use for binding the queue to an exchange.
        exchange_name (str): The name of the exchange to bind the queue to.
        bind (bool, optional): Whether or not to bind the queue to the exchange. Defaults to True.

    Returns:
        None
    """
    ch.queue_declare(queue_name)
    if bind:
        ch.queue_bind(
            exchange=exchange_name, queue=queue_name, routing_key=routing_key,
        )
    init_rmq_logger.debug(
        'Initialized RabbitMQ setup | Queue: {} | Exchange: {} | Routing Key: {}',
        queue_name,
        exchange_name,
        routing_key,
    )


def init_topic_exchange_and_queue(
    ch: pika.adapters.blocking_connection.BlockingChannel,
    exchange_name: str,
    queue_name: str,
    routing_key_pattern: str,
) -> None:
    """
    Initialize a topic exchange and queue in RabbitMQ.

    Args:
        ch (pika.adapters.blocking_connection.BlockingChannel): A blocking channel object for RabbitMQ.
        exchange_name (str): The name of the exchange to declare.
        queue_name (str): The name of the queue to declare.
        routing_key_pattern (str): The routing key pattern to use for the queue.

    Returns:
        None
    """
    # Declare the topic exchange
    ch.exchange_declare(
        exchange=exchange_name, exchange_type='topic', durable=True,
    )
    init_rmq_logger.debug(
        'Initialized RabbitMQ Topic exchange: {}', exchange_name,
    )

    # Initialize the queue and bind it to the exchange
    init_queue(
        ch=ch,
        exchange_name=exchange_name,
        queue_name=queue_name,
        routing_key=routing_key_pattern,
    )


def init_callback_queue(
    ch: pika.adapters.blocking_connection.BlockingChannel,
) -> None:
    """
    Initializes the callback queues for snapshot and aggregate.

    Args:
        ch (pika.adapters.blocking_connection.BlockingChannel): The blocking channel object.

    Returns:
        None
    """
    callback_exchange_name = f'{settings.rabbitmq.setup.callbacks.exchange}:{settings.namespace}'

    # Initialize snapshot queue
    queue_name, routing_key_pattern = get_snapshot_queue_routing_key_pattern()
    init_topic_exchange_and_queue(
        ch,
        exchange_name=callback_exchange_name,
        queue_name=queue_name,
        routing_key_pattern=routing_key_pattern,
    )

    # Initialize aggregate queue
    queue_name, routing_key_pattern = get_aggregate_queue_routing_key_pattern()
    init_queue(
        ch,
        exchange_name=callback_exchange_name,
        queue_name=queue_name,
        routing_key=routing_key_pattern,
    )


def init_commit_payload_queue(
    ch: pika.adapters.blocking_connection.BlockingChannel,
) -> None:
    """
    Initializes a RabbitMQ queue for commit payloads.

    Args:
        ch (pika.adapters.blocking_connection.BlockingChannel): The RabbitMQ channel to use.

    Returns:
        None
    """
    commit_payload_exchange_name = f'{settings.rabbitmq.setup.commit_payload.exchange}:{settings.namespace}'
    routing_key_pattern = f'powerloom-backend-commit-payload:{settings.namespace}:{settings.instance_id}.*'
    queue_name = f'powerloom-backend-commit-payload-queue:{settings.namespace}:{settings.instance_id}'

    init_topic_exchange_and_queue(
        ch,
        exchange_name=commit_payload_exchange_name,
        queue_name=queue_name,
        routing_key_pattern=routing_key_pattern,
    )


def init_delegate_worker_queue(
    ch: pika.adapters.blocking_connection.BlockingChannel,
) -> None:
    """
    Initializes the delegate worker queue by declaring the response and request exchanges
    and initializing the request queue.

    Args:
        ch (pika.adapters.blocking_connection.BlockingChannel): The blocking channel to use for
            declaring exchanges and initializing the queue.

    Returns:
        None
    """
    # Declare response exchange
    delegated_worker_response_exchange_name = (
        f'{settings.rabbitmq.setup.delegated_worker.exchange}:Response:{settings.namespace}'
    )
    ch.exchange_declare(
        exchange=delegated_worker_response_exchange_name, exchange_type='direct', durable=True,
    )

    # Declare request exchange
    delegated_worker_request_exchange_name = (
        f'{settings.rabbitmq.setup.delegated_worker.exchange}:Request:{settings.namespace}'
    )
    ch.exchange_declare(
        exchange=delegated_worker_request_exchange_name, exchange_type='direct', durable=True,
    )

    # Initialize request queue
    request_queue_name, request_queue_routing_key = get_delegate_worker_request_queue_routing_key()
    init_queue(
        ch,
        exchange_name=delegated_worker_request_exchange_name,
        queue_name=request_queue_name,
        routing_key=request_queue_routing_key,
        bind=True,
    )


def init_event_detector_queue(
    ch: pika.adapters.blocking_connection.BlockingChannel,
) -> None:
    """
    Initializes the event detector queue by creating a topic exchange and a queue
    with the given exchange name, queue name, and routing key pattern.

    Args:
        ch (pika.adapters.blocking_connection.BlockingChannel): The blocking channel
            to use for creating the exchange and queue.

    Returns:
        None
    """
    event_detector_exchange_name = f'{settings.rabbitmq.setup.event_detector.exchange}:{settings.namespace}'
    routing_key_pattern = f'powerloom-event-detector:{settings.namespace}:{settings.instance_id}.*'
    queue_name = f'powerloom-event-detector:{settings.namespace}:{settings.instance_id}'

    init_topic_exchange_and_queue(
        ch,
        exchange_name=event_detector_exchange_name,
        queue_name=queue_name,
        routing_key_pattern=routing_key_pattern,
    )


def init_exchanges_queues():
    """
    Initializes all RabbitMQ exchanges and queues required for the snapshotter.
    This includes the core exchange, processhub commands queue, callback queues,
    event detector queue, commit payload queue, and delegate worker queue.
    """
    # Create a new RabbitMQ connection
    c = create_rabbitmq_conn()
    ch: pika.adapters.blocking_connection.BlockingChannel = c.channel()

    # Initialize core exchange
    exchange_name = f'{settings.rabbitmq.setup.core.exchange}:{settings.namespace}'
    ch.exchange_declare(
        exchange=exchange_name, exchange_type='direct', durable=True,
    )
    init_rmq_logger.debug(
        'Initialized RabbitMQ Direct exchange: {}', exchange_name,
    )

    # Initialize other queues
    init_callback_queue(ch)
    init_event_detector_queue(ch)
    init_commit_payload_queue(ch)
    init_delegate_worker_queue(ch)


if __name__ == '__main__':
    init_exchanges_queues()
