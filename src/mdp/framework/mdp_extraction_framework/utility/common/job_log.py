"""Extraction Job Log Module."""

# import: standard
from enum import Enum


class JobStatus(Enum):
    """This class is enumerator for job status which can be only as following:

    - SUCCESS: Indicates that the job has completed successfully.
    - FAILED: Indicates that the job has failed.

    Args:
        Enum: Enum based class
    """

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
