"""Delta Table Utility."""

# import: standard
import fcntl
import logging
import os
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

# import: external
from deltalake import DeltaTable
from deltalake import write_deltalake
from pandas import DataFrame


class DeltaTableOperation:
    """A utility class to handle operations on Delta tables, including creation, writing
    data, loading data, and performing housekeeping operations."""

    def __init__(self) -> None:
        """Initialize a DeltaTableOperation instance.

        Sets up a logger for the class.
        """
        self.logger = logging.getLogger(self.__class__.__name__)

    def create_table_if_not_exist(
        self, table_uri: str, schema: Dict[str, str], **kwargs: Any
    ) -> DeltaTable:
        """Creates a Delta table at the specified URI if it does not already exist and
        returns the DeltaTable object. Uses DeltaTable.create to initialize the table.

        Args:
            table_uri (str): The URI of the Delta table directory.
            schema (Dict[str, str]): A dictionary defining the table schema (column name and data type).
            **kwargs (Any): Additional arguments for DeltaTable.create (e.g., 'partition_columns').

        Returns:
            DeltaTable: A DeltaTable object representing the table.
        """
        try:
            delta_table = DeltaTable(table_uri)
            self.logger.info(f"Delta table already exists at '{table_uri}'.")
        except Exception:
            self.logger.info(
                f"Delta table does not exist at '{table_uri}'. Creating a new table..."
            )
            DeltaTable.create(
                table_uri=table_uri,
                schema=schema,
                mode="overwrite",
                **kwargs,
            )
            self.logger.info(f"Delta table has been successfully created at '{table_uri}'.")
            delta_table = DeltaTable(table_uri)
        return delta_table

    def write_delta_table(
        self,
        delta_table: DeltaTable,
        data: DataFrame,
        mode: str = "append",
        partition_by: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> None:
        """Writes data to a Delta Lake table.

        Args:
            delta_table (DeltaTable): A DeltaTable object representing the Delta table.
            data (DataFrame): The data to write into the table.
            mode (str): Write mode (e.g., 'append', 'overwrite'). Defaults to 'append'.
            partition_by (Optional[List[str]]): List of column names to partition the table by. Defaults to None.
            **kwargs (Any): Additional arguments passed to write_deltalake.

        Returns:
            None
        """
        try:
            write_deltalake(
                table_or_uri=delta_table,
                data=data,
                mode=mode,
                partition_by=partition_by,
                **kwargs,
            )
            self.logger.info(f"Data written to Delta table '{delta_table}' in '{mode}' mode.")
        except Exception as e:
            self.logger.error(f"Failed to write to Delta table: {e}")
            raise

    def load_table_as_df(self, target_table: DeltaTable, **kwargs: Any) -> DataFrame:
        """Load data from a Delta Table as a Pandas DataFrame with optional filters.

        Args:
            target_table (DeltaTable): The target Delta Table to load data from.

        Returns:
            DataFrame: A filtered Pandas DataFrame loaded from the Delta Table.
        """
        try:
            df = target_table.to_pandas(**kwargs)
            self.logger.debug("Data loaded successfully from the Delta Table.")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load data from Delta Table: {e}")
            raise

    def is_num_files_over_threshold(self, path: str, file_threshold: int = 50) -> bool:
        """Checks if the number of files in a Delta table directory (and its
        subdirectories) exceeds the threshold.

        Args:
            path (str): Path to the Delta table directory.
            file_threshold (int): Maximum allowable number of files (default is 50).

        Returns:
            bool: True if the number of files exceeds the threshold, False otherwise.
        """
        num_files = sum(
            len(files)
            for _, _, files in os.walk(path)
            if any(f.endswith(".parquet") for f in files)
        )
        if num_files > file_threshold:
            self.logger.info(f"Number of files exceeds the threshold of {file_threshold}.")
            return True
        return False

    def acquire_lock(self, file_path: str):
        """Acquire an exclusive lock on a specified lock file to ensure no concurrent
        processes perform operations on the same Delta table.

        If the lock file does not exist, it will be created.

        Args:
            file_path (str): The path to the lock file.

        Returns:
            file: The open file object representing the lock file if the lock is acquired.
            None: If the lock cannot be acquired.
        """
        if not os.path.exists(file_path):
            open(file_path, "w").close()  # Create the file if it doesn't exist

        lock_file = open(file_path, "r+")
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.logger.info(f"Lock acquired on file: {file_path}")
            return lock_file
        except IOError:
            self.logger.info(f"Another process may be running on lock file {file_path}.")
            return None

    def release_lock(self, lock_file):
        """Release a previously acquired lock and delete the lock file.

        Args:
            lock_file (file): The open file object representing the lock file.

        Returns:
            None
        """
        try:
            lock_file.close()
            self.logger.info(f"Lock file {lock_file.name} released.")
        except Exception as e:
            self.logger.error(f"Error releasing lock file: {e}")

    def compact_and_clean_table(self, delta_table: DeltaTable, lock_file_path: str) -> None:
        """Compact and clean a Delta Table by performing optimization and vacuuming.

        Ensures that these operations are not performed concurrently by using a file-based lock.

        Args:
            delta_table (DeltaTable): A DeltaTable object representing the Delta table.
            lock_file_path (str): The path to the lock file.

        Returns:
            None
        """
        lock = self.acquire_lock(lock_file_path)
        if not lock:
            self.logger.info("Another compaction process is running. Exiting compaction.")
            return

        try:
            # Step 1: Compact Delta table
            operation_result = delta_table.optimize.compact()
            self.logger.info(f"Table optimization complete: {operation_result}")

            # Step 2: Vacuum the table to delete old files
            vacuum_result = delta_table.vacuum(
                retention_hours=0,
                enforce_retention_duration=False,
                dry_run=False,
            )
            self.logger.info(f"Table vacuum complete. Files removed: {len(vacuum_result)}")
        except Exception as e:
            self.logger.error(f"Compaction and cleanup failed: {e}")
            raise
        finally:
            self.release_lock(lock)
