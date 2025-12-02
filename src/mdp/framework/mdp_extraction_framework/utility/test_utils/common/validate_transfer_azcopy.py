"""Common Method for validating table component."""

# import: standard
import logging

# import: internal
from mdp.framework.mdp_extraction_framework.utility.shell_script.common import run_command


def validate_file_exists(
    storage_url: str,
    sas_token: str,
) -> bool:
    """Method to validate target database is exists.

    Args:
        spark (SparkSession): spark session
        catalog (str): catalog name
        job_name (str): job name for logging

    Returns:
        bool: Indicating whether the database exists
    """
    # Setup logger for logging commands
    logger = logging.getLogger(__name__)

    # Define azcopy list file command line using storage URL path
    az_list_file_command = f"azcopy list '{storage_url}?{sas_token}'"
    logger.info(f"azcopy list command: {storage_url}?<SAS_TOKEN>")

    # Run azcopy list commandline
    command_result = run_command(az_list_file_command)

    # Check exit_code And log output
    if command_result.exit_code == 0:
        logger.info(f"Show azcopy output list: \n{command_result.output}")

        # Split the cmd output each line into a list
        adls_files = command_result.output.split("\n")

        # Get the ADLS file list from the cmd output
        adls_files = [
            row.replace("INFO: ", "").split(";")[0].split("/")[-1].strip(".") for row in adls_files
        ]
        logger.info(f"List of files on the ADLS: {adls_files}")

        return adls_files

    else:
        raise ValueError(
            f"Azcopy returned an error with exit code: {command_result.exit_code}, output: {command_result.output}"
        )
