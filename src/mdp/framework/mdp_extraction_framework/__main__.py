"""Control Framework Main."""

# import: standard
import argparse
import json
import logging
import sys
from datetime import datetime

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import ConfigMapping
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.job_param.job_param_converter import (
    add_value_to_job_param,
)
from mdp.framework.mdp_extraction_framework.job_param.job_param_converter import modify_job_param

# from mdp.framework.mdp_extraction_framework.operation_log.extraction_oper_log import (
#     ExtractionPipelineOperLog,
# )
from mdp.framework.mdp_extraction_framework.pipeline.extraction import ExtractionPipeline  # noqa
from mdp.framework.mdp_extraction_framework.pipeline.extraction import (
    ExtractionPipelineExecutedValues,
)
from mdp.framework.mdp_extraction_framework.utility.common.job_log import JobStatus
from mdp.framework.mdp_extraction_framework.utility.common_function import get_class_object
from mdp.framework.mdp_extraction_framework.utility.common_function import setup_logger
from mdp.framework.mdp_extraction_framework.utility.file_reader.config_reader import JSONReader

# import: external
from dotenv import load_dotenv


def entrypoint(argv: list = sys.argv[1:]):
    """Entrypoint for pipeline.

    Args:
        argv (list): A list of parameters to be parsed if provided, use sys.argv[1:] otherwise.
    """
    parser = argparse.ArgumentParser(description="MDP Control Framework")
    parser.add_argument("--project", help="Project name (mdp, oih)", required=False, default="mdp")
    parser.add_argument("--config_file_path", help="config file path", required=True)
    parser.add_argument("--pos_dt", help="Data Date", required=True)
    parser.add_argument("--adb_job_id", help="Databricks Workflow Parent ID", required=False)
    parser.add_argument("--adb_run_id", help="Unique Databricks Workflow Run ID", required=False)
    parser.add_argument("--scheduler_id", help="Scheduler Id", required=False)
    parser.add_argument(
        "--overwrite_config",
        help='Overwriting config \'tasks\' with input string. Input keys and values with escaped double-quotes, for single-quote inside double-quotes please use unicode (\\u0027), eg: \'{"record_count_check_task":{"parameters":{"raw_filter_cond":"asd=\u0027{{pos_dt}}\u0027"}}}\' ',
        type=json.loads,
        required=False,
        default={},
    )
    parser.add_argument(
        "--run_only_task",
        help="Specify the task(s) to run. Provide the task name(s) as a comma-separated list to execute only the selected task(s) and skip others. Example: --run-only-task task1,task2",
        required=False,
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose output", required=False
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

    # Validate CLI arguments
    job_param = JobParameters(**vars(system_arguments))

    # Read Config
    json_reader = JSONReader(config_file_path=job_param.config_file_path)
    template_config = json_reader.read_file()
    overwritten_config = json_reader.overwrite_config(
        template_config, system_arguments.overwrite_config
    )

    # Modify job param
    dict_config = json.loads(overwritten_config)
    if "modify_job_param" in dict_config:
        job_param = modify_job_param(dict_config["modify_job_param"], job_param)

    config = json_reader.render_jinja_template(
        overwritten_config, ConfigMapping(pos_dt=job_param.pos_dt)
    )
    job_param = add_value_to_job_param(config, job_param)

    # Initialize application logging
    logger = (
        setup_logger(
            job_name=job_param.job_name, pos_dt=job_param.pos_dt, verbose=system_arguments.verbose
        )
        if not logging.getLogger().hasHandlers()
        else logging.getLogger(__name__)
    )

    # Log the modified job_param if 'modify_job_param' is in the dict config
    if "modify_job_param" in dict_config:
        logger.info(f"Modified Job Parameters to: {job_param}")
    logger.info(f"Template Config: {template_config}")
    logger.info(f"Overwritten Config: {overwritten_config}")

    # Get pipeline callable class
    pipeline_cls = get_class_object(__name__, job_param.pipeline_name)
    error: Exception = None
    job_start_datetime = datetime.now()
    executed_values = ExtractionPipelineExecutedValues()
    job_status = None
    job_message = None

    try:
        # Execution pipeline
        pipeline = pipeline_cls(job_parameters=job_param, config=config)
        executed_values = pipeline.execute()
        job_status = JobStatus.SUCCESS.value
    except Exception as e:
        error = e
        logger.exception(error)
        job_status = JobStatus.FAILED.value
        job_message = f"{str(error.__class__.__name__)}: {str(error)}"
    finally:
        job_end_datetime = datetime.now()
        logger.info(
            f"""
        {'=' * 30}
        Extraction Job Log Summary
        job_nm|pos_dt|scheduler_id|job_start_datetime|job_end_datetime|job_status|job_message|area_nm|job_seq|extract_file_path|target_file_path|files_size
        {job_param.job_name}|{job_param.pos_dt}|{job_param.scheduler_id}|{job_start_datetime}|{job_end_datetime}|{job_status}|{job_message}|{job_param.area_name}|{job_param.job_seq}|{executed_values.extract_file_path}|{executed_values.target_file_path}|{executed_values.files_size}
        {'=' * 30}
        """
        )
        logger.info(
            f"""
        {'=' * 30}
        Extraction Control File Details
        {executed_values.ctl_file_details}
        {'=' * 30}
        """
        )
        # extraction_oper_log = ExtractionPipelineOperLog()
        # extraction_oper_log.create_log_table_if_not_exist()
        # extraction_oper_log.insert_log(
        #     job_param,
        #     job_start_datetime,
        #     job_end_datetime,
        #     job_status,
        #     job_message,
        #     executed_values,
        # )
        # extraction_oper_log.housekeeping()
        if error is not None:
            raise error


if __name__ == "__main__":
    entrypoint()
