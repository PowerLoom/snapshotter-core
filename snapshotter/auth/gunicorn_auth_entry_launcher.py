"""
Gunicorn Auth Entry Launcher

This module sets up and launches a Gunicorn server for the authentication service.
It configures logging, sets up workers, and initializes the application.
"""

import logging
import os

from snapshotter.auth.conf import auth_settings
from snapshotter.auth.server_entry import app
from snapshotter.utils.gunicorn import InterceptHandler
from snapshotter.utils.gunicorn import StandaloneApplication
from snapshotter.utils.gunicorn import StubbedGunicornLogger

# Configuration variables from environment
JSON_LOGS = True if os.environ.get('JSON_LOGS', '0') == '1' else False
LOG_LEVEL = logging.getLevelName(os.environ.get('LOG_LEVEL', 'DEBUG'))
WORKERS = int(os.environ.get('GUNICORN_WORKERS', '5'))


if __name__ == '__main__':
    # Set up logging
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

    # Gunicorn server options
    options = {
        'bind': f'{auth_settings.bind.host}:{auth_settings.bind.port}',
        'workers': WORKERS,
        'accesslog': '-',  # Log to stdout
        'errorlog': '-',   # Log to stderr
        'worker_class': 'uvicorn.workers.UvicornWorker',
        'logger_class': StubbedGunicornLogger,
    }

    # Run the Gunicorn application
    StandaloneApplication(app, options).run()
