"""Common Function Module for shell script."""

# import: standard
import shlex
import subprocess
from dataclasses import dataclass


@dataclass
class CommandResult:
    """Results from the shell's command line output."""

    output: str
    error: str
    exit_code: int


def run_command(command) -> CommandResult:
    """Execute a shell command and return output, error, and exit_code from the
    command's result.

    Args:
        command (str): a string of shell command

    Returns:
        CommandResult: shell command's output, error, and exit_code
    """
    try:
        command_list = shlex.split(command)
        result = subprocess.run(command_list, capture_output=True, text=True, shell=False)
        # Get the command's output, error and exit_code
        output = result.stdout.strip()
        error = result.stderr.strip()
        exit_code = result.returncode

        # Create a return dictionary from the command's results
        # command_result = {"output": output, "error": error, "exit_code": exit_code}
        command_result = CommandResult(output=output, error=error, exit_code=exit_code)
    except Exception as e:
        command_result = CommandResult(output="", error=str(e), exit_code=1)

    return command_result
