import logging
import tempfile
from pathlib import Path
from urllib import parse

import requests
import boto3

logging.basicConfig(level="INFO")
s3_client = boto3.client('s3')


def get_and_upload_data(file_url, s3_uri):
    with tempfile.TemporaryDirectory() as local_path:
        parsed_url = parse.urlparse(file_url)
        file_name = Path(parsed_url.path).name
        file_path = Path(local_path) / file_name
        with file_path.open("wb") as f, requests.get(file_url, stream=True) as r:
            for chunk in r.iter_content():
                f.write(chunk)
        logging.info(f"Retrieved {file_url}")
        bucket, key = s3_uri.replace("s3://", "").split("/", 1)

        s3_client.upload_file(
            Filename=file_path.as_posix(),
            Bucket=bucket,
            Key=key
        )
        logging.info(f"Uploaded to {s3_uri}")