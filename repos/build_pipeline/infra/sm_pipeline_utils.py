import importlib
import json
import logging
import os
from pathlib import Path
from typing import List, Union

import boto3
import sagemaker

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_pipeline_props(file_path: Union[str, Path]) -> dict:
    with file_path.open("r") as f:
        pipeline_properties = json.load(f)

    return pipeline_properties


def get_session(region: str, default_bucket: str) -> sagemaker.session.Session:
    """Gets the sagemaker session based on the region.
    Args:
        region: the aws region to start the session
        default_bucket: the bucket to use for storing the artifacts

    Returns:
        `sagemaker.session.Session instance
    """

    boto_session = boto3.Session(region_name=region)

    sagemaker_client = boto_session.client("sagemaker")
    runtime_client = boto_session.client("sagemaker-runtime")
    try:
        sagemaker.session.Session(
            boto_session=boto_session,
            sagemaker_client=sagemaker_client,
            sagemaker_runtime_client=runtime_client,
            default_bucket=default_bucket,
        )
        logger.info("SageMaker Session created")
    except:
        logger.exception("Failed to generate a SageMaker Session")

    return sagemaker.session.Session(
        boto_session=boto_session,
        sagemaker_client=sagemaker_client,
        sagemaker_runtime_client=runtime_client,
        default_bucket=default_bucket,
    )


def generate_pipeline_definition(
    role: str,
    region: str,
    default_bucket: str,
    pipeline_name: str,
    pipeline_conf: dict,
    code_file_path: Union[str, Path],
) -> str:
    """Generates a SageMaker pipeline definition

    Args:
        role (str): ARN of the role assumed by the pipeline steps
        region (str): region
        default_bucket (str): deafult bucket to upload artifacts
        pipeline_name (str): name to give to the pipeline
        pipeline_conf (dict): configuration of the pipeline

    Returns:
        [str]: pipeline definition as a json object
    """
    if not isinstance(code_file_path, Path):
        code_file_path = Path(code_file_path)
    try:
        module = importlib.import_module(
            "." + code_file_path.stem, package=code_file_path.parent.as_posix()
        )
        logger.info("Loading the pipeline definition module")
    except:
        logger.exception("Failed to load the Pipeline definion module")
        return

    logger.info("Creating SageMaker Session")
    sm_session = get_session(region=region, default_bucket=default_bucket)
    logger.info(
        f"Creating SageMaker Pipeline definition. Artifacts to be uploaded in {default_bucket}"
    )
    pipeline = module.get_pipeline(
        role, pipeline_name, sagemaker_session=sm_session, **pipeline_conf
    )
    logger.info("Uploading generated defintion")
    definition = pipeline.definition()
    logger.debug(json.dumps(json.loads(definition), indent=2, sort_keys=True))

    return definition


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    project_bucket_name = os.getenv("PROJECT_BUCKET")
    execution_role_arn = os.getenv("SAGEMAKER_PIPELINE_ROLE_ARN")
    project_name = os.getenv("SAGEMAKER_PROJECT_NAME")
    project_id = os.getenv("SAGEMAKER_PROJECT_ID")

    conf_path = Path("configurations")

    pipeline_props = get_pipeline_props(conf_path / "claims.pipeline.json")
    pipeline_conf = pipeline_props["pipeline_configuration"]

    pipeline = generate_pipeline_definition(
        role=execution_role_arn,
        region="ap-southeast-1",
        default_bucket=project_bucket_name,
        pipeline_name="test",
        pipeline_conf=pipeline_conf,
        code_file_path=pipeline_props["code_file_path"],
    )
    print(json.dumps(json.loads(pipeline), indent=2))
