"""Job Parameter Base Model."""
# import: standard
from typing import Optional

# import: external
from pydantic import BaseModel


class JobParameters(BaseModel):
    """Job Parameter base model.

    Args:
        BaseModel: pydantic basemodel
    """

    project: Optional[str] | None = "mdp"
    pos_dt: str
    config_file_path: str
    adb_job_id: Optional[str] | None = ""
    adb_run_id: Optional[str] | None = ""
    scheduler_id: Optional[str] | None = ""
    job_info: Optional[dict] | None = {}
    job_name: Optional[str] | None = ""
    area_name: Optional[str] | None = ""
    job_seq: Optional[int] | None = 0
    pipeline_name: Optional[str] | None = ""
    run_only_task: Optional[str] | None = ""
    # TODO: validate pos_dt format

    # TODO: discuss on this step, especially when reading config
