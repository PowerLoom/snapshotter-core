"""
Gunicorn Core Launcher

This module sets up and runs a Gunicorn server for the Core API application.
It configures logging, workers, and other server options based on environment variables.
"""

import logging
import os
import sys

from snapshotter.core_api import app
from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import FORMAT
from snapshotter.utils.default_logger import logger
from snapshotter.utils.gunicorn import InterceptHandler
from snapshotter.utils.gunicorn import StandaloneApplication
from snapshotter.utils.gunicorn import StubbedGunicornLogger

# Configure logging level from environment variable, defaulting to DEBUG
LOG_LEVEL = logging.getLevelName(os.environ.get('LOG_LEVEL', 'DEBUG'))

# Set number of Gunicorn workers from environment variable, defaulting to 5
WORKERS = int(os.environ.get('GUNICORN_WORKERS', '5'))

# Enable JSON logging if JSON_LOGS environment variable is set to '1'
JSON_LOGS = True if os.environ.get('JSON_LOGS', '0') == '1' else False


if __name__ == '__main__':
    # Set up logging interceptor
    intercept_handler = InterceptHandler()
    logging.root.setLevel(LOG_LEVEL)

    # Configure logging for various modules
    seen = set()
    for name in [
        *logging.root.manager.loggerDict.keys(),
        'gunicorn',
        'gunicorn.access',
        'gunicorn.error',
        'uvicorn',
        'uvicorn.access',
        'uvicorn.error',
    ]:
        if name not in seen:
            seen.add(name.split('.')[0])
            logging.getLogger(name).handlers = [intercept_handler]

    # Add logging handlers for stdout and stderr
    logger.add(sys.stdout, format=FORMAT, level=LOG_LEVEL, serialize=JSON_LOGS)
    logger.add(sys.stderr, format=FORMAT, level=logging.ERROR, serialize=JSON_LOGS)

    # Configure Gunicorn server options
    options = {
        'bind': f'{settings.core_api.host}:{settings.core_api.port}',
        'workers': WORKERS,
        'accesslog': '-',  # Log to stdout
        'errorlog': '-',   # Log to stderr
        'worker_class': 'uvicorn.workers.UvicornWorker',
        'logger_class': StubbedGunicornLogger,
    }

    # Run the Gunicorn server with the configured options
    StandaloneApplication(app, options).run()
