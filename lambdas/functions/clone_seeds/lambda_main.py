import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import boto3
import cfnresponse
from aws_lambda_powertools import Logger

logger = Logger()
s3 = boto3.resource("s3")

bucket = os.getenv("SeedBucket")


@logger.inject_lambda_context(log_event=True)
def lambda_handler(event, context):
    response_status = cfnresponse.SUCCESS
    try:
        if "RequestType" in event and event["RequestType"] == "Create":
            logger.info("Processing CREATE event")
            response_data = on_create(event, context)
        if "RequestType" in event and event["RequestType"] == "Update":
            logger.info("Processing UPDATE event")
            response_data = on_create(event, context)
        if "RequestType" in event and event["RequestType"] == "Delete":
            logger.info("Processing DELETE event")
            response_data = no_op(event, context)
    except Exception:
        logger.exception("Something went wrong")
        response_status = cfnresponse.FAILED
        response_data = {}

    cfnresponse.send(event, context, response_status, response_data, "")


def on_create(event, _):
    with tempfile.TemporaryDirectory() as td:
        base_dir = Path(td)
        props = event["ResourceProperties"]

        git_repo = props["GitRepository"]

        branch = None
        if "Branch" in props:
            branch = props["Branch"]

        git_clone_bash(url=git_repo, to_path=base_dir.as_posix(), branch=branch)

        try:
            seed_paths = props["SeedPaths"]
            seed_keys = [seed_code_upload(base_dir / k) for k in seed_paths]
        except:
            logger.exception("No Seed Code path provided")

        try:
            template_path = props["TemplatePath"]
            template_keys = templates_upload(base_dir / template_path)
        except:
            logger.exception("No Template path provided")

    return dict(
        seed_keys=seed_keys,
        template_key=template_keys,
    )


def no_op(_, __):
    pass


def templates_upload(template_path: Path):
    key = Path(template_path).name
    s3_o = s3.Object(bucket_name=bucket, key=key)
    s3_o.upload_file(template_path)
    logger.info(f"Uploaded {template_path} to s3://{bucket}/{key}")


def seed_code_upload(dir_path: Path):
    with tempfile.NamedTemporaryFile() as tf:
        archive = shutil.make_archive(
            base_name=tf.name,
            format="zip",
            root_dir=dir_path,
            base_dir=".",
        )
        logger.info(f"Compressing {dir_path} into {archive}")
        key = Path(dir_path).name + ".zip"
        s3_o = s3.Object(bucket_name=bucket, key=key)
        s3_o.upload_file(archive)
    logger.info(f"Uploaded {archive} to s3://{bucket}/{key}")
    return s3_o.key


def git_clone_bash(url: str, to_path: str, branch: str = None):
    cmd = ["git", "clone", "--depth=1"]
    if branch is not None:
        cmd += ["-b", branch]
    cmd += [url, to_path]
    subprocess.run(cmd)
