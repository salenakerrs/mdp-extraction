"""Base Data Extractor Module."""

# import: standard
import os
from datetime import datetime
from typing import Optional

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.task.base_task import Task

# import: external
from pydantic import BaseModel


class DataFileInformation(BaseModel):
    """Model representing information about the generated file.

    Args:
        file_location (str): The path to the file location.
        file_size (int): The file size
        file_created_datetime (datetime): The datetime of the file creation.
    """

    file_location: str
    file_size: int
    file_created_datetime: Optional[datetime]


def generate_data_file_info(file_location: str) -> DataFileInformation:
    """Generate a DataFileInformation object.

    Args:
        file_location (str): genearted file path

    Returns:
        DataFileInformation: DataFileInformation object representing file detail.
    """
    file_info = DataFileInformation(
        file_location=file_location,
        file_size=os.path.getsize(file_location),
        file_created_datetime=datetime.now(),
    )
    return file_info


class BaseDataExtractorTask(Task):
    """Base Data Extractor Task, for fetching data from MDP.

    Args:
        Task (class): The base class for defining tasks in the workflow.
    """

    def __init__(self, module_config: dict, job_parameters: JobParameters) -> None:
        """Initializes a BaseDataExtractorTask instance.

        Args:
            module_config (dict): A dictionary containing module configuration.
            job_parameters (JobParameters): An object containing job parameters.
        """
        super().__init__(module_config, job_parameters)
