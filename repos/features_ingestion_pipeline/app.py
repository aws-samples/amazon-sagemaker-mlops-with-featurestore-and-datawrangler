import os

from aws_cdk import App, Tags, DefaultStackSynthesizer
from infra.features_ingestion_stack import FeatureIngestionStack

project_name = os.getenv("SAGEMAKER_PROJECT_NAME")
project_id = os.getenv("SAGEMAKER_PROJECT_ID")


app = App()

bucket_name = os.getenv("PROJECT_BUCKET")
region = os.getenv("AWS_REGION")

synth = DefaultStackSynthesizer(
    file_assets_bucket_name=bucket_name, bucket_prefix="feature_ingestion/"
)

fs_stack = FeatureIngestionStack(
    app,
    f"{project_name}-FeatureStore",
    configuration_path="configurations",
    synthesizer=synth,
)

Tags.of(fs_stack).add(key="sagemaker:project-id", value=project_id)
Tags.of(fs_stack).add(key="sagemaker:project-name", value=project_name)

app.synth()
