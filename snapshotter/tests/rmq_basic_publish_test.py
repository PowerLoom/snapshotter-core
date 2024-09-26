import queue
import threading

import pika

from snapshotter.init_rabbitmq import create_rabbitmq_conn
from snapshotter.settings.config import settings
from snapshotter.utils.rabbitmq_helpers import RabbitmqThreadedSelectLoopInteractor


def interactor_wrapper_obj(rmq_q: queue.Queue):
    """
    Wrapper function to create and run a RabbitmqThreadedSelectLoopInteractor.

    Args:
        rmq_q (queue.Queue): Queue for RabbitMQ messages.
    """
    rmq_interactor = RabbitmqThreadedSelectLoopInteractor(publish_queue=rmq_q)
    rmq_interactor.run()


if __name__ == '__main__':
    # Initialize queue for RabbitMQ messages
    q = queue.Queue()

    # Define command to be sent
    CMD = (
        '{"command": "start", "pid": null, "proc_str_id":'
        ' "EpochCallbackManager", "init_kwargs": {}}'
    )

    # Set up exchange and routing key
    exchange = f'{settings.rabbitmq.setup.core.exchange}:{settings.namespace}'
    routing_key = f'processhub-commands:{settings.namespace}'

    try:
        # Start the RabbitMQ interactor in a separate thread
        t = threading.Thread(target=interactor_wrapper_obj, kwargs={'rmq_q': q})
        t.start()

        # Get user input for publish method
        i = input(
            '1 for vanilla pika adapter publish. 2 for select loop adapter publish',
        )
        i = int(i)

        if i == 1:
            # Publish using vanilla pika adapter
            c = create_rabbitmq_conn()
            ch = c.channel()
            ch.basic_publish(
                exchange=f'{settings.rabbitmq.setup.core.exchange}:{settings.namespace}',
                routing_key=f'processhub-commands:{settings.namespace}',
                body=CMD.encode('utf-8'),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='text/plain',
                    content_encoding='utf-8',
                ),
                mandatory=True,
            )
            print('Published to rabbitmq')
        else:
            # Publish using select loop adapter
            print('Trying to publish via select loop adapter...')
            broadcast_msg = (CMD.encode('utf-8'), exchange, routing_key)
            q.put(broadcast_msg)
    except KeyboardInterrupt:
        # Gracefully stop the thread on keyboard interrupt
        t.join()
