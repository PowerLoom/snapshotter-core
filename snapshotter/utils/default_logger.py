from loguru import logger
from snapshotter.settings.config import settings

# Format string for log messages. {extra} field can be used to pass extra parameters to the logger using .bind()
FORMAT = '{time:MMMM D, YYYY > HH:mm:ss!UTC} | {level} | {message} | {extra}'

def create_level_filter(level):
    """
    Create a filter function for a specific log level.

    Args:
        level (str): The log level to filter for.

    Returns:
        function: A filter function that returns True for the specified log level.
    """
    def filter_func(record):
        return record["level"].name == level
    return filter_func

# Create filter functions for each log level
logger_filter_trace = create_level_filter("TRACE")
logger_filter_debug = create_level_filter("DEBUG")
logger_filter_info = create_level_filter("INFO")
logger_filter_success = create_level_filter("SUCCESS")
logger_filter_warning = create_level_filter("WARNING")
logger_filter_error = create_level_filter("ERROR")
logger_filter_critical = create_level_filter("CRITICAL")

def trace_enabled(_):
    """
    Returns the value of trace_enabled setting from the settings module.

    Args:
        _: Unused argument.

    Returns:
        bool: The value of trace_enabled setting.
    """
    return settings.logs.trace_enabled


def configure_file_logging():
    """
    Configure file logging for different log levels if enabled in settings.
    """
    log_levels = [
        ("debug", "DEBUG", logger_filter_debug),
        ("info", "INFO", logger_filter_info),
        ("success", "SUCCESS", logger_filter_success),
        ("warning", "WARNING", logger_filter_warning),
        ("error", "ERROR", logger_filter_error),
        ("critical", "CRITICAL", logger_filter_critical),
        ("trace", "TRACE", logger_filter_trace),
    ]

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


# Remove any pre-existing handlers
logger.remove()

# Configure file logging if enabled in settings
if settings.logs.write_to_files:
    configure_file_logging()
