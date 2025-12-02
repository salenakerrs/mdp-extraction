"""Test azcopy_data_transfer."""
# import: standard

# import: internal
from mdp.framework.mdp_extraction_framework.task.data_transfer.azcopy_data_transfer import (
    ADLSLocation,
)
from mdp.framework.mdp_extraction_framework.task.data_transfer.azcopy_data_transfer import (
    LocalLocation,
)
from mdp.framework.mdp_extraction_framework.task.data_transfer.azcopy_data_transfer import (
    validate_transfer_file,
)
from mdp.framework.mdp_extraction_framework.utility.shell_script.common import CommandResult

# import: external
import pytest


def test_LocalLocation():
    """Test method to the 'get_sas_token' private method in LocalLocation class."""
    # build actual DataFrame
    type = "LocalLocation"
    data_location = "test_location/test_file.txt"

    expected_data_filepath = "test_location/test_file.txt"

    # Override abstract methods of the class for testing
    LocalLocation.__abstractmethods__ = frozenset()
    local_location = LocalLocation(type=type, filepath=data_location)

    assert (
        local_location.filepath == expected_data_filepath
    ), f"The returned filepath: {local_location.filepath} does not match the expected filepath: {expected_data_filepath}"


def test_ADLSLocation_update_adls_filepath_url():
    """Test method for the 'update_adls_filepath_url' private method in ADLSLocation
    class."""
    # build actual DataFrame
    type = "ADLSLocation"
    account_name = "stmteststorage001"
    container_name = "inbnd"
    sas_token = "test_token"
    filepath = "test_location/"
    cleanup_file_pattern = "test_file*"

    expected_adls_filepath_no_token = (
        "https://stmteststorage001.blob.core.windows.net/inbnd/test_location/"
    )
    expected_adls_filepath = f"{expected_adls_filepath_no_token}?test_token"
    # Override abstract methods of the class for testing
    ADLSLocation.__abstractmethods__ = frozenset()
    adls_location = ADLSLocation(
        type=type,
        account_name=account_name,
        container_name=container_name,
        sas_token=sas_token,
        filepath=filepath,
        cleanup_file_pattern=cleanup_file_pattern,
    )
    adls_filepath_url = adls_location.update_adls_filepath_url()

    assert (
        adls_filepath_url.filepath_without_token == expected_adls_filepath_no_token
    ), f"The returned filepath {adls_filepath_url.filepath_without_token} does not match the expected filepath: {expected_adls_filepath_no_token}"

    assert (
        adls_filepath_url.filepath == expected_adls_filepath
    ), f"The returned filepath {adls_filepath_url.filepath} does not match the expected filepath: {expected_adls_filepath}"


def test_validate_transfer_file_success():
    """Test the function 'validate_transfer_file' for cases where there is no error from
    the output."""
    mock_command_result = CommandResult(
        output="""Diagnostic stats:
    IOPS: 3
    End-to-end ms per request: 210
    Network Errors: 0.00%
    Server Busy: 0.00%


    Job test-id summary
    Elapsed Time (Minutes): 0.0333
    Number of File Transfers: 2
    Number of Folder Property Transfers: 0
    Number of Symlink Transfers: 0
    Total Number of Transfers: 2
    Number of File Transfers Completed: 2
    Number of Folder Transfers Completed: 0
    Number of File Transfers Failed: 0
    Number of Folder Transfers Failed: 0
    Number of File Transfers Skipped: 0
    Number of Folder Transfers Skipped: 0
    TotalBytesTransferred: 1598
    Final Job Status: Completed
    """,
        exit_code=0,
        error="",
    )

    validate_transfer_file(mock_command_result)


@pytest.mark.parametrize(
    "mock_command_result",
    [
        CommandResult(
            output="""Diagnostic stats:
        IOPS: 3
        End-to-end ms per request: 210
        Network Errors: 0.00%
        Server Busy: 0.00%


        Job test-id summary
        Elapsed Time (Minutes): 0.0333
        Number of File Transfers: 2
        Number of Folder Property Transfers: 0
        Number of Symlink Transfers: 0
        Total Number of Transfers: 2
        Number of File Transfers Completed: 2
        Number of Folder Transfers Completed: 0
        Number of File Transfers Failed: 1
        Number of Folder Transfers Failed: 1
        Number of File Transfers Skipped: 0
        Number of Folder Transfers Skipped: 0
        TotalBytesTransferred: 1598
        Final Job Status: Completed
        """,
            exit_code=1,
            error="Mock error message",
        ),
        CommandResult(
            output="""Diagnostic stats:
        IOPS: 3
        End-to-end ms per request: 210
        Network Errors: 0.00%
        Server Busy: 0.00%


        Job test-id summary
        Elapsed Time (Minutes): 0.0333
        Number of File Transfers: 0
        Number of Folder Property Transfers: 0
        Number of Symlink Transfers: 0
        Total Number of Transfers: 0
        Number of File Transfers Completed: 0
        Number of Folder Transfers Completed: 0
        Number of File Transfers Failed: 0
        Number of Folder Transfers Failed: 0
        Number of File Transfers Skipped: 0
        Number of Folder Transfers Skipped: 0
        TotalBytesTransferred: 0
        Final Job Status: Completed
        """,
            exit_code=0,
            error="",
        ),
    ],
)
def test_validate_transfer_file_failure(mock_command_result):
    """Test the function 'validate_transfer_file' for failure cases, whether exit_code
    is 0 or 1."""

    with pytest.raises(ValueError):
        validate_transfer_file(mock_command_result)
