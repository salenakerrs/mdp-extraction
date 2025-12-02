"""Common Method for validating files component."""

# import: standard
import csv
import os
from typing import Union


def validate_local_file_exists(filepath: str) -> bool:
    """Method to checks if a file exists at the given filepath.

    Args:
        filepath (str): The path to the file.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    return os.path.exists(filepath)


def validate_csv_header(
    filepath: str, expected_header: list, delimiter: str = "|", quoting: int = csv.QUOTE_ALL
) -> bool:
    """Method to validate csv header match with expected.

    Args:
        filepath (str): csv file path
        expected_header (list): expected header
        delimiter (str): The delimiter used in the CSV file. Default is "|".
        quoting (int): Quoting style, defaults to csv.QUOTE_ALL.

    Returns:
        bool: True if match expected, false otherwise.
    """
    with open(filepath, "r", newline="") as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=delimiter, quoting=quoting)
        header = next(csv_reader, None)
    return header == expected_header


def get_csv_row_count(filepath: str, delimiter: str = "|", quoting: int = csv.QUOTE_ALL) -> int:
    """Read the row count of a CSV file.

    Args:
        filepath (str): csv file path
        expected_header (list): expected header
        delimiter (str): The delimiter used in the CSV file. Default is "|".
        quoting (int): Quoting style, defaults to csv.QUOTE_ALL.

    Returns:
        int: Number of rows in the CSV file.
    """
    with open(filepath, "r", newline="") as csvfile:
        csvreader = csv.reader(csvfile, delimiter=delimiter, quoting=quoting)
        row_count = sum(1 for row in csvreader)
    return row_count


def get_csv_column_value(
    csv_file_path: str, column_name: str, delimiter: str = "|", quoting: int = csv.QUOTE_ALL
) -> Union[str, None]:
    """Get the value from a specific column in a CSV file with one row.

    Parameters:
        csv_file_path (str): The path to the CSV file.
        column_name (str): The name of the column to retrieve.
        delimiter (str): The delimiter used in the CSV file. Default is "|".
        quoting (int): Quoting style, defaults to csv.QUOTE_ALL.

    Returns:
        str or None: The value from the specified column, or None if the column is not found.
    """
    with open(csv_file_path, "r", newline="") as csvfile:
        csv_reader = csv.DictReader(csvfile, delimiter=delimiter, quoting=quoting)
        row = next(csv_reader, None)
        if row and column_name in row:
            return row[column_name]
    return None
