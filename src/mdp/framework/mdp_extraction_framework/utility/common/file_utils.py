"""File utility functions."""
# import: standard
import os
from pathlib import Path
from typing import List
from typing import Union


def cleanup_files(file_list: List[Union[str, Path]]) -> None:
    """Remove the specified list of files.

    Args:
        file_list (List[Union[str, Path]]): List of file paths to remove.

    Raises:
        ValueError: If any of the paths do not exist or are not files.
        Exception: If removal fails for any file.
    """
    for file_path in file_list:
        file_path = Path(file_path)

        if not file_path.exists():
            raise ValueError(f"File does not exist: {file_path}")
        if not file_path.is_file():
            raise ValueError(f"Path is not a file: {file_path}")

        try:
            os.remove(file_path)
        except Exception as e:
            raise Exception(f"Failed to remove file {file_path}: {e}")
