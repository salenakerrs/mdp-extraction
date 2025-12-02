"""Zip File Extractor Test Module."""
# import: standard
import os
import shutil
from pathlib import Path

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.task.file_extractor.zip_file_extractor import (
    ZipFileExtractorTask,
)
from mdp.framework.mdp_extraction_framework.task.file_extractor.zip_file_extractor import (
    ZipFileExtractorTaskConfigModel,
)

# import: external
import pytest
from pydantic import BaseModel

JOB_PARAMS = JobParameters(
    pos_dt="2023-10-31",
    config_file_path="",
)


class mock_model(BaseModel, extra="allow"):
    """A mock pydantic model."""

    pass


@pytest.fixture
def mock_zip(tmp_path):
    """Creates a temporary zip file for testing."""
    test_zip_path = tmp_path / "test.zip"
    with open(tmp_path / "testfile.txt", "w") as f:
        f.write("This is a test file.")
    shutil.make_archive(str(test_zip_path).replace(".zip", ""), "zip", tmp_path, "testfile.txt")

    yield test_zip_path


@pytest.fixture
def zip_file_extractor_task(mock_zip):
    """Returns an instance of ZipFileExtractorTask."""
    param = {"source_file_location": str(mock_zip)}
    module_config = mock_model(
        module_name=ZipFileExtractorTask, parameters=ZipFileExtractorTaskConfigModel(**param)
    )
    return ZipFileExtractorTask(module_config=module_config, job_parameters=JOB_PARAMS)


def test_make_tmp_dir(zip_file_extractor_task, tmp_path):
    """Tests that make_tmp_dir creates the expected temporary directory."""
    tmp_folder_location = zip_file_extractor_task.make_tmp_dir(f"{tmp_path}/test.zip")
    expected_tmp_folder_location = os.path.join(f"{tmp_path}", "_tmp_test")
    assert (
        tmp_folder_location == expected_tmp_folder_location
    ), "Temporary directory location mismatch."
    assert Path(tmp_folder_location).exists(), "Temporary directory was not created."
