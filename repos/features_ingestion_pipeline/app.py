import os

from aws_cdk import core as cdk
from infra.features_ingestion_stack import FeatureIngestionStack

project_name = os.getenv("SAGEMAKER_PROJECT_NAME")
project_id = os.getenv("SAGEMAKER_PROJECT_ID")


app = cdk.App()

bucket_name = os.getenv("PROJECT_BUCKET")
region = os.getenv("AWS_REGION")

synth = cdk.DefaultStackSynthesizer(
    file_assets_bucket_name=bucket_name, bucket_prefix="feature_ingestion/"
)

fs_stack = FeatureIngestionStack(
    app,
    f"{project_name}-FeatureStore",
    configuration_path="configurations",
    synthesizer=synth,
)

cdk.Tags.of(fs_stack).add(key="sagemaker:project-id", value=project_id)
cdk.Tags.of(fs_stack).add(key="sagemaker:project-name", value=project_name)

app.synth()
