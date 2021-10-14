import os

from aws_cdk import core as cdk

from infra.serving_stack import ServingStack

project_name = os.getenv("SAGEMAKER_PROJECT_NAME")
project_id = os.getenv("SAGEMAKER_PROJECT_ID")

app = cdk.App()

bucket_name = os.getenv("PROJECT_BUCKET")
region = os.getenv("AWS_REGION")

synth = cdk.DefaultStackSynthesizer(
    file_assets_bucket_name=bucket_name, bucket_prefix="serving/"
)

serving_stack = ServingStack(
    app,
    f"{project_name}-ServingStack",
    configuration_path="configurations",
    synthesizer=synth
)
cdk.Tags.of(serving_stack).add(key="sagemaker:project-id", value=project_id)
cdk.Tags.of(serving_stack).add(key="sagemaker:project-name", value=project_name)
app.synth()
