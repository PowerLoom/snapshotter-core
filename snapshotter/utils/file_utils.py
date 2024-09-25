import json
import os
from typing import Any

from loguru import logger

from snapshotter.settings.config import settings

default_logger = logger.bind(module='Powerloom|FileUtils')


def read_json_file(
    file_path: str,
    logger: logger = default_logger,
) -> dict:
    """
    Read a JSON file and return its content as a dictionary.

    Args:
        file_path (str): The path to the JSON file to read.
        logger (logger, optional): The logger to use for logging. Defaults to default_logger.

    Returns:
        dict: The content of the JSON file as a dictionary.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        Exception: If there's an error opening or reading the file.
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f'File {file_path} not found')

    try:
        # Attempt to open the file
        with open(file_path, 'r', encoding='utf-8') as f_:
            json_data = json.load(f_)
            
            # Handle cases where the JSON might be nested in strings
            while not isinstance(json_data, dict) and isinstance(json_data, str):
                json_data = json.loads(json_data)
            
            return json_data
    except Exception as exc:
        logger.warning(f'Unable to open or read the {file_path} file')
        if settings.logs.trace_enabled:
            logger.opt(exception=True).error(exc)
        raise exc


def write_json_file(
    directory: str,
    file_name: str,
    data: Any,
    logger: logger = logger,
) -> None:
    """
    Write data to a JSON file at the specified directory with the specified file name.

    Args:
        directory (str): The directory where the file will be created.
        file_name (str): The name of the file to be created.
        data (Any): The data to be written to the file.
        logger (logger, optional): The logger object to be used for logging. Defaults to logger.

    Raises:
        Exception: If there is an error while creating the directory or writing to the file.

    Returns:
        None
    """
    file_path = os.path.join(directory, file_name)
    
    try:
        # Create directory if it doesn't exist
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        # Write data to file
        with open(file_path, 'w', encoding='utf-8') as f_:
            json.dump(data, f_, ensure_ascii=False, indent=4)
    except Exception as exc:
        logger.error(f'Unable to write to file {file_path}')
        raise exc


def write_bytes_to_file(directory: str, file_name: str, data: bytes) -> None:
    """
    Write bytes to a file in the specified directory.

    Args:
        directory (str): The directory where the file will be written.
        file_name (str): The name of the file to be written.
        data (bytes): The bytes to be written to the file.

    Raises:
        Exception: If the directory cannot be created or the file cannot be opened/written.

    Returns:
        None
    """
    file_path = os.path.join(directory, file_name)
    
    try:
        # Create directory if it doesn't exist
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        # Write bytes to file
        with open(file_path, 'wb') as file_obj:
            bytes_written = file_obj.write(data)
            logger.debug('Wrote {} bytes to file {}', bytes_written, file_path)
    except Exception as exc:
        logger.opt(exception=True).error('Unable to open or write to the {} file', file_path)
        raise exc


def read_text_file(file_path: str) -> str | None:
    """
    Read the given file and return the contents as a string.

    Args:
        file_path (str): The path to the file to be read.

    Returns:
        str | None: The contents of the file as a string, or None if the file could not be read.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            return file_obj.read()
    except FileNotFoundError:
        logger.warning('File not found: {}', file_path)
        return None
    except Exception as exc:
        logger.opt(exception=True).warning('Unable to open the {} file because of exception: {}', file_path, exc)
        return None
