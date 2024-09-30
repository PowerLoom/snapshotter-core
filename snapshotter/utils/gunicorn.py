import logging

from gunicorn.app.base import BaseApplication
from gunicorn.glogging import Logger

from snapshotter.utils.default_logger import default_logger


logger = default_logger.bind(module='Gunicorn')


class InterceptHandler(logging.Handler):
    """
    A custom logging handler that intercepts log records and forwards them to Loguru default_logger.

    This handler is designed to bridge the gap between Python's standard logging
    and the Loguru logger, allowing for seamless integration of both logging systems.
    """

    def emit(self, record):
        """
        Emit a log record by forwarding it to the Loguru logger.

        :param record: The log record to be emitted
        :type record: logging.LogRecord
        """
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        # Log the message using Loguru
        logger.opt(depth=depth, exception=record.exc_info).log(
            level,
            record.getMessage(),
        )


class StubbedGunicornLogger(Logger):
    """
    A custom logger for Gunicorn that stubs out the error and access loggers.

    This logger sets up a NullHandler for both the error and access loggers, effectively
    disabling them.
    """

    def setup(self, cfg):
        """
        Set up the logger with NullHandlers and configure log levels.

        :param cfg: Gunicorn configuration object
        :type cfg: gunicorn.config.Config
        """
        handler = logging.NullHandler()

        # Set up error logger
        self.error_logger = logging.getLogger('gunicorn.error')
        self.error_logger.addHandler(handler)

        # Set up access logger
        self.access_logger = logging.getLogger('gunicorn.access')
        self.access_logger.addHandler(handler)


class StandaloneApplication(BaseApplication):
    """
    A standalone Gunicorn application that can be run without a Gunicorn server.

    This class allows for programmatic configuration and running of a Gunicorn server
    with a given WSGI application.
    """

    def __init__(self, app, options=None):
        """
        Initialize the Gunicorn server with the given app and options.

        :param app: The WSGI application to run
        :type app: callable
        :param options: Optional dictionary of configuration options
        :type options: dict
        """
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        """
        Load the configuration for the Gunicorn server.

        This function loads the configuration for the Gunicorn server from the options
        provided by the user. It sets the configuration values in the `cfg` object.
        """
        config = {
            key: value
            for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        """
        Load the application and return it.

        :return: The WSGI application
        :rtype: callable
        """
        return self.application
