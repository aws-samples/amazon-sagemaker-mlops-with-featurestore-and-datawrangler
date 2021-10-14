import os
from time import sleep

import boto3
import botocore
import logging

project_bucket_name = os.getenv("PROJECT_BUCKET")
region = os.getenv("AWS_REGION")

client_s3 = boto3.client(service_name="s3")
resource_s3 = boto3.resource("s3")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def upload_file_to_bucket(
    source_file_full_path: str, target_bucket_name: str, target_key_name: str
):
    """Upload target file

    Args:
        source_file_full_path (str): [description]
        target_bucket_name (str): [description]
        target_key_name (str): [description]

    Raises:
        ex: [description]
    """

    logger.info(
        f"Upload local file [{source_file_full_path}] to S3 [{target_bucket_name}/{target_key_name}] ..."
    )
    # logger.info(f"cwd <{os.getcwd()}>")
    # logger.info(f"listdir <{os.listdir(os.getcwd())}>")

    try:
        resource_s3.Bucket(target_bucket_name).upload_file(
            source_file_full_path, target_key_name
        )
        sleep(5)
    except botocore.exceptions.ClientError as ex:
        logger.exception("Failed to upload Glue script")
        raise ex

    logger.info("Upload done!")