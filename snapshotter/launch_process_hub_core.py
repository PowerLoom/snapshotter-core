import signal

from snapshotter.init_rabbitmq import init_exchanges_queues
from snapshotter.process_hub_core import ProcessHubCore
from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import logger
from snapshotter.utils.exceptions import GenericExitOnSignal


def generic_exit_handler(signum, frame):
    """
    A signal handler function that raises a GenericExitOnSignal exception.

    This function is designed to be used as a callback for signal handling,
    specifically for graceful shutdown of the process.

    Args:
        signum (int): The signal number received.
        frame (frame): The current stack frame at the time the signal was received.

    Raises:
        GenericExitOnSignal: Always raised to initiate the shutdown process.
    """
    raise GenericExitOnSignal


def main():
    """
    Main function to launch and manage the ProcessHubCore.

    This function performs the following tasks:
    1. Sets up signal handlers for SIGINT, SIGTERM, and SIGQUIT.
    2. Initializes logging for the launcher.
    3. Sets up RabbitMQ exchanges and queues.
    4. Creates and starts a ProcessHubCore instance.
    5. Waits for the ProcessHubCore to complete, handling any shutdown signals.

    The function uses a try-except-finally structure to ensure proper shutdown
    and logging in case of normal termination or signal-induced shutdown.
    """
    # Set up signal handlers for graceful shutdown
    for signame in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
        signal.signal(signame, generic_exit_handler)

    # Initialize logging
    # Using bind to pass extra parameters to the logger, will show up in the {extra} field
    launcher_logger = logger.bind(
        module='Powerloom|SnapshotterProcessHub|Core|Launcher',
        namespace=settings.namespace,
        instance_id=settings.instance_id[:5],
    )

    # Initialize RabbitMQ exchanges and queues
    init_exchanges_queues()

    # Create and start ProcessHubCore
    p_name = f'Powerloom|SnapshotterProcessHub|Core-{settings.instance_id[:5]}'
    core = ProcessHubCore(name=p_name)
    core.start()
    launcher_logger.debug('Launched {} with PID {}', p_name, core.pid)

    try:
        # Wait for the core process to complete
        launcher_logger.debug(
            '{} Launcher still waiting on core to join...',
            p_name,
        )
        core.join()
    except GenericExitOnSignal:
        # Handle shutdown signal
        launcher_logger.debug(
            (
                '{} Launcher received SIGTERM. Will attempt to join with'
                ' ProcessHubCore process...'
            ),
            p_name,
        )
    finally:
        # Ensure proper shutdown and logging
        try:
            launcher_logger.debug(
                '{} Launcher still waiting on core to join...',
                p_name,
            )
            core.join()
        except Exception as e:
            launcher_logger.info(
                ('{} Launcher caught exception still waiting on core to' ' join... {}'),
                p_name,
                e,
            )
        launcher_logger.debug(
            '{} Launcher found alive status of core: {}',
            p_name,
            core.is_alive(),
        )


if __name__ == '__main__':
    main()
