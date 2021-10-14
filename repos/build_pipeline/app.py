import os

from aws_cdk import core as cdk

from infra.build_model_stack import BuildModelStack

project_name = os.getenv("SAGEMAKER_PROJECT_NAME")
project_id = os.getenv("SAGEMAKER_PROJECT_ID")


app = cdk.App()

bucket_name = os.getenv("PROJECT_BUCKET")
region = os.getenv("AWS_REGION")

synth = cdk.DefaultStackSynthesizer(
    file_assets_bucket_name=bucket_name, bucket_prefix="build_model/"
)

build_stack = BuildModelStack(
    app,
    f"{project_name}-BuildModelStack",
    configuration_path="configurations",
    synthesizer=synth,
)
cdk.Tags.of(build_stack).add(key="sagemaker:project-id", value=project_id)
cdk.Tags.of(build_stack).add(key="sagemaker:project-name", value=project_name)

app.synth()
