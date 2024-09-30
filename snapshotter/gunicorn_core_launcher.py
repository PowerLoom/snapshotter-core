import os

from snapshotter.core_api import app
from snapshotter.settings.config import settings
from snapshotter.utils.gunicorn import StandaloneApplication
from snapshotter.utils.gunicorn import StubbedGunicornLogger

WORKERS = int(os.environ.get('GUNICORN_WORKERS', '5'))
JSON_LOGS = True if os.environ.get('JSON_LOGS', '0') == '1' else False

if __name__ == '__main__':

    options = {
        'bind': f'{settings.core_api.host}:{settings.core_api.port}',
        'workers': WORKERS,
        'accesslog': '-',
        'errorlog': '-',
        'worker_class': 'uvicorn.workers.UvicornWorker',
        'logger_class': StubbedGunicornLogger,
    }

    StandaloneApplication(app, options).run()
