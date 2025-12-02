"""Control Framework Utils."""

# import: standard
import argparse
import logging
import sys

# import: internal
from mdp.framework.mdp_extraction_framework.utility.test_utils.connection.connectivity import (
    ConnectivityTest,
)

# import: external
from dotenv import load_dotenv


def setup_logger(verbose: bool = False) -> None:
    """Sets up logger stream and level.

    Args:
        verbose (bool, optional): verbose debug output. Defaults to False.
    """
    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    stdout_handler.setLevel(logging.DEBUG if verbose else logging.INFO)

    stderr_handler = logging.StreamHandler(stream=sys.stderr)
    stderr_handler.setLevel(logging.ERROR)

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(filename)s | %(name)s | %(lineno)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        handlers=[stderr_handler, stdout_handler],
    )


def validate_args(parser, args):
    """Validate the arguments provided to the script.

    Args:
        parser (argparse.ArgumentParser): The parser object used to parse the arguments.
        args (argparse.Namespace): The arguments provided to the script.
    """
    if not any(vars(args).values()):
        parser.error(
            "No arguments provided. Please provide valid arguments to run the script. See --help or -h for more details."
        )

    if args.connectivity_test and not args.connection_name:
        parser.error("--connection_name is required when --connectivity_test is provided.")

    if not args.connectivity_test and args.query:
        parser.error("--query is only valid when --connectivity_test is provided.")


def entrypoint(argv: list = sys.argv[1:]):
    """Entrypoint for pipeline.

    Args:
        argv (list): A list of parameters to be parsed if provided, use sys.argv[1:] otherwise.
    """
    parser = argparse.ArgumentParser(description="Extraction Framework Utils")

    parser.add_argument("--project", help="Project name (mdp, oih)", required=False, default="mdp")

    conn_parser_group = parser.add_argument_group("Connectivity Test Options")
    conn_parser_group.add_argument(
        "--connectivity_test",
        action="store_true",
        help="Run a connectivity test, --connection_name is required.",
        required=False,
    )
    conn_parser_group.add_argument(
        "--connection_name",
        help="Specify the connection name when running a connectivity test.",
        required=False,
    )
    conn_parser_group.add_argument(
        "--query",
        help="Optional query to execute during the connectivity test.",
        required=False,
    )
    conn_parser_group.add_argument(
        "--query_file_path",
        help="Optional query file path to execute during the connectivity test.",
        required=False,
    )

    system_arguments = parser.parse_args(argv)

    # Load environment variables dynamically based on project
    project = system_arguments.project.lower()

    project_root_map = {
        "mdp": "app_mdp",
        "oih": "app_oih",
    }

    try:
        root_path = project_root_map[project]
    except KeyError:
        raise ValueError(f"Unsupported project: {project}")

    load_dotenv(f"/{root_path}/{project}/script/extraction/.env", override=True)
    load_dotenv(f"/{root_path}/{project}/script/extraction/.env.secret", override=True)

    validate_args(parser=parser, args=system_arguments)

    # Initialize application logging
    setup_logger()
    logger = logging.getLogger(__name__)
    test_status = "FAILED"
    query_result = None

    try:
        if system_arguments.connectivity_test is True:
            test_status, query_result = ConnectivityTest(
                connection_name=system_arguments.connection_name,
                query=system_arguments.query,
                query_file_path=system_arguments.query_file_path,
            )
            logger.info("Connectivity Test Result: %s", test_status)
            if system_arguments.query or system_arguments.query_file_path:
                if query_result:
                    logger.info("Query Test Result: SUCCESS")
                    logger.info("Query Result: %s", query_result)
                else:
                    logger.info("Query Test Result: FAILED")

        else:
            logger.info("No valid arguments provided, exiting script.")

    except Exception as e:
        error = e
        logger.exception(error)


if __name__ == "__utils__":
    entrypoint()
