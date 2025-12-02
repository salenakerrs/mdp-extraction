"""Entrypoint Module."""

# import: standard
import argparse
import os
import sys

# import: external
import pytest


def run_pytest(repo_root: str, test_path: str, report_path: str) -> None:
    """Method to run pytest.

    Args:
        repo_root (str): root directory of repository
        test_path (str): path to run pytest tests
        report_path (str): path to save report file
    """

    # Skip writing pyc files on a readonly filesystem.
    sys.dont_write_bytecode = True

    # set report file path & version

    # import: internal
    from mdp.framework.mdp_extraction_framework.__init__ import __version__  # noqa

    # Create the report directory if it doesn't exist
    os.makedirs(report_path, exist_ok=True)
    report_file = f"{report_path}/v{__version__}/sit/mdp_extraction_framework.html"

    # Run pytest.
    retcode = pytest.main(
        [
            test_path,
            "-vv",
            f"--html={report_file}",
            "--self-contained-html",
            "-p",
            "no:cacheprovider",
        ]
    )

    # Fail the cell execution if there are any test failures.
    exit_messages = {
        0: "SIT run complete, all tests PASSED!",
        1: "SIT run complete, some tests FAILED!",
        2: "SIT run interrupted by the user!",
        3: "Internal error occurred while executing tests!",
        4: "Usage error: pytest command line usage error!",
        5: "No tests were collected!",
    }

    if retcode in exit_messages:
        if retcode != 0:
            sys.exit(f"Error Code {retcode}: {exit_messages[retcode]}")


def entrypoint(argv: list = sys.argv[1:]):
    """Entrypoint for pytest.

    Args:
        argv (list): A list of parameters to be parsed if provided, use sys.argv[1:] otherwise.
    """
    parser = argparse.ArgumentParser(description="This is the entry point to run pytest.")
    parser.add_argument(
        "--repo_root", help="root directory of repository", required=False, default=""
    )
    parser.add_argument(
        "--test_path", help="path to run pytest tests", required=False, default="test/oih/sit"
    )
    parser.add_argument(
        "--report_path",
        help="path to save report file",
        required=False,
        default="~/oih_extraction_sit_report",
    )
    system_arguments, unknown_arguments = parser.parse_known_args(argv)
    run_pytest(**vars(system_arguments))


if __name__ == "__main__":
    entrypoint()
