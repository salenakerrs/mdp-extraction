"""Common Function module."""
# import: standard
import logging
import os
import sys
from datetime import datetime
from inspect import getmembers
from inspect import isclass
from operator import methodcaller
from pathlib import Path
from typing import Callable
from typing import List


def bypass_wrapper(func: Callable) -> Callable:
    """Decorator to conditionally execute a method based on a bypass flag.

    This decorator checks the `bypass_flag` attribute of the object it is applied to.
    If the `bypass_flag` is `True`, the decorated method is executed.
    If the `bypass_flag` is `False`, the method returns `None`.

    Args:
        func (Callable): The method to be decorated.

    Returns:
        Callable: The decorated method.
    """

    def wrapper(self, *args, **kwargs):
        if self.bypass_flag is True:
            return None
        return func(self, *args, **kwargs)

    return wrapper


def get_class_object(module_name: str, input_class_name: str) -> Callable:
    """Take a `input_class_name` to search for the corresponding class in the current
    module. If found, return the class object.

    Args:
        module_name (str): module name of the module calling this method
        input_class_name (str): String of status log table class to retrieve.

    Returns:
        Callable: Class object corresponding to the specified input.

    Raises:
        KeyError: If the specified `input_class_name` does not correspond to any existing class.
    """
    for class_name, class_object in getmembers(sys.modules[module_name], isclass):
        if input_class_name == class_name:
            return class_object
    raise KeyError(f"Class {input_class_name} does not exist.")


def get_log_filename(
    job_name: str,
    pos_dt: str,
    file_name: str = "extraction_fw",
    root_log_directory: str = None,
) -> tuple[Path, str]:
    """Generates a directory path and a filename with the current year-month, job name,
    and optional custom filename.

    Args:
      job_name (str): The name of the job for the log file.
      pos_dt (str): Date of the running job.
      file_name (str, optional): The filename for the log file. Defaults to "extraction_fw.log".
      root_log_directory (str, optional): The root directory to store log files.

    Returns:
      A tuple containing the directory path (Path) and the filename (str).
    """
    current_datetime = datetime.now()
    current_year_month = current_datetime.strftime("%Y-%m")
    directory_base = root_log_directory

    if not directory_base:
        project = os.getenv("PROJECT", "mdp").lower()
        directory_base = f"/app_log_{project}/{project}/extraction/"

    directory = Path(directory_base, current_year_month, job_name)
    current_timestamp = current_datetime.strftime("%Y%m%d%H%M%S")
    pos_dt_datetime = datetime.strptime(pos_dt, "%Y-%m-%d")
    pos_dt_str = pos_dt_datetime.strftime("%Y%m%d")
    if job_name:
        file_name = f"{file_name}_{job_name}"
    if pos_dt:
        file_name = f"{file_name}_{pos_dt_str}"
    final_file_name = f"{file_name}_{current_timestamp}.log"
    return directory, final_file_name


def setup_logger(
    job_name: str,
    pos_dt: str,
    verbose: bool = False,
    format_string: str = "%(asctime)s | %(levelname)s | %(filename)s | %(name)s | %(lineno)s | %(message)s",
) -> logging.Logger:
    """Sets up a logger with a file handler writing to a folder named with the current
    year-month and a subfolder named after the job name, alongside stream handlers for
    stdout and stderr.

    Args:
      job_name (str): The name of the job for the log file.
      pos_dt (str): Date of the running job.
      verbose (bool, optional): Enable verbose mode with DEBUG logging level. Defaults to False.
      format_string (str, optional): The format string for log messages. Defaults to "%(asctime)s - %(name)s - %(levelname)s - %(message)s".

    Returns:
      logging.Logger: a logger object of the new logger
    """

    # remove root's handler for the below handlers to be usable
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Get directory and filename
    directory, filename = get_log_filename(job_name, pos_dt)

    # Create the directory if it doesn't exist
    os.makedirs(directory, exist_ok=True)

    # Configure logger with all handlers and desired level
    log_level = logging.DEBUG if verbose else logging.INFO

    # Create stream handlers for stdout and stderr with desired format
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(log_level)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.ERROR)

    # Create file handler with desired format
    file_handler = logging.FileHandler(Path(directory, filename))
    file_handler.setLevel(log_level)

    logging.basicConfig(
        level=log_level,
        format=format_string,
        handlers=[stdout_handler, stderr_handler, file_handler],
    )

    # Get a new logger with the configured handlers
    logger = logging.getLogger(__name__)

    return logger


def convert_config_to_tuples(config: dict, parent_key=None):
    """Converts a nested dictionary into a list of tuples representing configuration
    items.

    Args:
        config (dict): The input dictionary to be converted.
        parent_key (str, optional): The parent key used during recursion. Defaults to an empty string.

    Returns:
        list: A list of tuples representing the configuration items.
    """
    converted_config = []

    for key, value in config.items():
        if isinstance(value, dict):
            converted_config.extend(convert_config_to_tuples(config=value, parent_key=key))
        else:
            config_tuple = (parent_key, key, value) if parent_key else (key, value)
            converted_config.append(config_tuple)

    return converted_config


def chain_callable_methods(chained_object: Callable, method_params_list: list[tuple]) -> Callable:
    """Chains methods or functions given the `method_params_list` of an input callable
    object.

    Args:
        chained_object (Callable): The input callable object to be chained.
        method_params_list (list[tuple]): List of tuple of method or function calls to be chained.

    Returns:
        Callable: Callable object resulting from chaining the specified methods or functions.
    """

    for method_params in method_params_list:
        method_name, *params = method_params
        chained_object = methodcaller(method_name, *params)(chained_object)

    return chained_object


def read_file(file_path: str) -> str:
    """Method to read content of a file.

    Returns:
        str: content of a file
    """
    with open(file_path, "r") as file:
        file_content = file.read()
    return file_content


def remove_files(file_paths: List[str]) -> None:
    """Remove a list of files.

    Args:
        file_paths (List[str]): A list of file paths to be removed.

    Raises:
        OSError: If an error occurs during file removal.
    """
    for file_path in file_paths:
        os.remove(file_path)
