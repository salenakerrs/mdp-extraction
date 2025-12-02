"""Job param converter Module."""

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.utility.date.common import get_holiday
from mdp.framework.mdp_extraction_framework.utility.date.common import get_offset_businessdays


def modify_job_param(modify_job_param: dict, job_param: JobParameters) -> JobParameters:
    """Modified value in JobParameters based on config from user.

    Args:
        modify_job_param (dict): Overwrite job param from user provided from job config
        job_param (JobParameters): JobParameters object

    Returns:
        JobParameters: JobParameters object after modified
    """

    if modify_job_param.get("source_type"):
        if modify_job_param["source_type"] == "lpm":
            holidays = get_holiday()
            pos_dt = get_offset_businessdays(job_param.pos_dt, holidays, offset=-1)
            job_param.pos_dt = pos_dt

        else:
            raise ValueError("Input source_type is not correct.")
    return job_param


def add_value_to_job_param(config: dict, job_param: JobParameters) -> JobParameters:
    """Add value to job param.

    Args:
        config (dict): job config dictionary
        job_param (JobParameters): JobParameters object

    Returns:
        JobParameters: JobParameters object after modified
    """
    job_param.job_name = config.get("job_name")
    job_param.job_info = config.get("job_info")
    job_param.area_name = config.get("area_name")
    job_param.job_seq = config.get("job_seq")
    job_param.pipeline_name = config.get("pipeline_name")

    return job_param
