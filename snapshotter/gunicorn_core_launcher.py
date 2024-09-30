from gunicorn.app.base import BaseApplication
from gunicorn.glogging import Logger

from snapshotter.utils.default_logger import default_logger

logger = default_logger.bind(module='Gunicorn')


class StubbedGunicornLogger(Logger):
    """
    A custom logger for Gunicorn that uses the pre-defined loguru logger.
    This logger sets up the error and access loggers using the existing loguru logger.
    """

    def setup(self, cfg):
        """
        Set up the logger with the pre-defined loguru logger and configure log levels.
        :param cfg: Gunicorn configuration object
        :type cfg: gunicorn.config.Config
        """
        # Set up error logger
        self.error_logger = logger.bind(name='gunicorn.error')

        # Set up access logger
        self.access_logger = logger.bind(name='gunicorn.access')


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
