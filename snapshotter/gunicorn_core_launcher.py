"""
Gunicorn Core Launcher
This module sets up and runs a Gunicorn server for the Core API application.
It configures logging with our singleton logger, workers, and other server options based on environment variables.
"""
import logging
import os

from snapshotter.core_api import app
from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.gunicorn import StandaloneApplication
from snapshotter.utils.gunicorn import StubbedGunicornLogger

# Configure logging level from environment variable, defaulting to DEBUG
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')

# Set number of Gunicorn workers from environment variable, defaulting to 5
WORKERS = int(os.environ.get('GUNICORN_WORKERS', '5'))


class InterceptHandler(logging.Handler):
    """
    Custom logging handler to intercept standard logging and redirect to Loguru.

    This handler captures logs from the standard logging module and forwards them
    to Loguru, maintaining consistent logging across the application.
    """

    def emit(self, record):
        """
        Emit a log record.

        This method is called by the logging system to emit a log record. It translates
        the standard log record into a Loguru log entry.

        Args:
            record (logging.LogRecord): The log record to be emitted.
        """
        # Get corresponding Loguru level if it exists
        try:
            level = default_logger.level(record.levelname).name
        except ValueError:
            # If the level name is not recognized, use the numeric level
            level = record.levelno

        # Find caller's frame to get accurate line numbers
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        # Log the message using Loguru with the appropriate context
        default_logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logging():
    """
    Set up logging for the application.
    """
    # Intercept standard logging
    logging.basicConfig(handlers=[InterceptHandler()], level=0)

    # Intercept uvicorn and gunicorn logging
    for _logger in ['uvicorn', 'uvicorn.access', 'uvicorn.error', 'gunicorn', 'gunicorn.access', 'gunicorn.error']:
        logging.getLogger(_logger).handlers = [InterceptHandler()]


if __name__ == '__main__':
    # Set up logging
    setup_logging()

    # Configure Gunicorn server options
    options = {
        'bind': f'{settings.core_api.host}:{settings.core_api.port}',
        'workers': WORKERS,
        'accesslog': '-',  # Log to stdout
        'errorlog': '-',  # Log to stderr
        'worker_class': 'uvicorn.workers.UvicornWorker',
        'logger_class': StubbedGunicornLogger,
    }

    # Run the Gunicorn server with the configured options
    StandaloneApplication(app, options).run()
