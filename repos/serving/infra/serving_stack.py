import json
import os
from pathlib import Path
from typing import List, Union

import aws_cdk as cdk
from aws_cdk import aws_apigateway as apigateway
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from constructs import Construct

from infra.batch_transform_construct import BatchTransform
from infra.model_endpoint_construct import ModelEndpointConstruct
from infra.redeploy_construct import Redeploy

project_bucket_name = os.getenv("PROJECT_BUCKET")
project_name = os.getenv("SAGEMAKER_PROJECT_NAME")
project_id = os.getenv("SAGEMAKER_PROJECT_ID")
codepipeline_arn = os.getenv("CODEPIPELINE_ARN")
execution_role_arn = os.getenv("SAGEMAKER_PIPELINE_ROLE_ARN")
events_role_arn = os.getenv("LAMBDA_ROLE_ARN")
glue_role_arn = os.getenv("GLUE_ROLE_ARN")
api_gateway_role_arn = os.getenv("API_GATEWAY_ROLE_ARN")
lambda_role_arn = os.getenv("LAMBDA_ROLE_ARN")

class ServingStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        configuration_path: Union[str, Path],
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)


        # Create SageMaker Pipeline
        sagemaker_execution_role = iam.Role.from_role_arn(
            self, "SageMakerExecutionRole", role_arn=execution_role_arn
        )

        if not isinstance(configuration_path, Path):
            configuration_path = Path(configuration_path)

        project_bucket = s3.Bucket.from_bucket_name(
            self, "ProjectBucket", bucket_name=project_bucket_name
        )

        api_gw = apigateway.RestApi(
            self,
            f"{project_name}-api",
            rest_api_name=f"{project_name}-api",
            endpoint_configuration={"types": [apigateway.EndpointType.REGIONAL]},
            description=f"API Endpoint for {project_name}",
        )
        api_gw.root.add_method("GET")

        for k in configuration_path.glob("*.model.json"):
            model_conf = get_model_conf(k)

            model_name = model_conf["model_name"]
            model_package_group_name = (
                f"{project_name}-{model_conf['model_package_group_name']}"
            )
            features_names = model_conf['features_names']

            Redeploy(
                self,
                f"RedeployConstruct-{model_name}",
                model_package_group_name=model_package_group_name,
            )

            for endpoint_conf in model_conf["endpoints"]:
                ModelEndpointConstruct(
                    self,
                    f"Endpoint-{endpoint_conf['endpoint_name']}",
                    model_package_group_name=model_package_group_name,
                    endpoint_conf=endpoint_conf,
                    api_gw=api_gw,
                )

            for transform_conf in model_conf["batch_transforms"]:
                BatchTransform(
                    self,
                    f"BatchTransform-{transform_conf['pipeline_name']}",
                    sagemaker_execution_role=sagemaker_execution_role,
                    project_bucket=project_bucket,
                    model_package_group_name=model_package_group_name,
                    pipeline_props=transform_conf,
                    features_names = features_names,
                    api_gw=api_gw,
                )


def get_model_conf(file_path: Union[str, Path]) -> dict:
    with file_path.open("r") as f:
        model_conf = json.load(f)

    return model_conf
