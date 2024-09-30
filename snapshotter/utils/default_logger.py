import sys
from functools import lru_cache

from loguru import logger

from snapshotter.settings.config import settings

# Format string for log messages
FORMAT = '{time:MMMM D, YYYY > HH:mm:ss!UTC} | {level} | {message} | {extra}'


def create_level_filter(level):
    """
    Create a filter function for a specific log level.
    """
    def filter_func(record):
        return record['level'].name == level
    return filter_func


# Create filter functions for each log level
logger_filter_trace = create_level_filter('TRACE')
logger_filter_debug = create_level_filter('DEBUG')
logger_filter_info = create_level_filter('INFO')
logger_filter_success = create_level_filter('SUCCESS')
logger_filter_warning = create_level_filter('WARNING')
logger_filter_error = create_level_filter('ERROR')
logger_filter_critical = create_level_filter('CRITICAL')


@lru_cache(maxsize=None)
def get_logger():
    """
    Configure and return the logger instance.
    This function is cached, so it will only configure the logger once.
    """
    # Remove any pre-existing handlers
    logger.remove()

    # Configure file logging
    log_levels = [
        ('trace', 'TRACE', logger_filter_trace),
        ('debug', 'DEBUG', logger_filter_debug),
        ('info', 'INFO', logger_filter_info),
        ('success', 'SUCCESS', logger_filter_success),
        ('warning', 'WARNING', logger_filter_warning),
        ('error', 'ERROR', logger_filter_error),
        ('critical', 'CRITICAL', logger_filter_critical),
    ]

    if settings.logs.write_to_files:
        for file_name, level, filter_func in log_levels:
            logger.add(
                f'logs/{file_name}.log',
                level=level,
                format=FORMAT,
                filter=filter_func,
                rotation='6 hours',
                compression='tar.xz',
                retention='2 days',
            )

    # Configure console logging
    logger.add(
        sys.stderr,
        level='TRACE',  # Set to lowest level
        format=FORMAT,
        filter=lambda record: (
            record['level'].no >= logger.level('INFO').no or
            (record['level'].no >= logger.level('DEBUG').no and settings.logs.trace_enabled)
        ),
    )

    return logger


# Usage
default_logger = get_logger()
