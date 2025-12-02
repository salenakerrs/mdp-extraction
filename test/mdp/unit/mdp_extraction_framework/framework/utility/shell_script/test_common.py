"""config_reader tests."""

# import: internal
from mdp.framework.mdp_extraction_framework.utility.shell_script.common import run_command

# import: external
import pytest


@pytest.mark.parametrize(
    "command, expected_exit_code, expected_output, expected_error",
    [
        ("echo test_command", [0], "test_command", ""),
        ("cd non_exist_directory", [1, 2], "", ""),
    ],
    ids=["success", "failure"],
)
def test_run_command(command, expected_exit_code, expected_output, expected_error):
    """Test the function 'run_command' that runs the given string of shell command."""

    command_result = run_command(command)

    # Validate the exit_code from the command run
    assert (
        command_result.exit_code in expected_exit_code
    ), f"The return exit_code: {command_result.exit_code} does not match the expected exit_code: 0"

    # Validate the output from the command run
    assert (
        command_result.output == expected_output
    ), f"The return output: {command_result.output} does not match the expected output: 'test_command'"

    # Validate the erorr message from the command run
    if expected_exit_code == [0]:
        assert (
            command_result.error == expected_error
        ), f"The return error: {command_result.error} does not match the expected error: '' (no error)"
    else:
        # Checking if the error is not an empty-string, the error message can be different based on the OS
        assert command_result.error != "", "The return error should not be None or empty-string ''"
