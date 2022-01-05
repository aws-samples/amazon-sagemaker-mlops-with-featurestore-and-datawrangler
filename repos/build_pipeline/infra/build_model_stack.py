import logging
import os
from pathlib import Path
from typing import List, Union

from aws_cdk import aws_codepipeline as codepipeline
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as events_targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_sagemaker as sagemaker
from aws_cdk import aws_ssm as ssm
from aws_cdk import core as cdk

from infra.sm_pipeline_utils import generate_pipeline_definition, get_pipeline_props

project_bucket_name = os.getenv("PROJECT_BUCKET")
pipeline_construct_id = os.getenv("CODEPIPELINE_CONSTRUCT_ID")
project_name = os.getenv("SAGEMAKER_PROJECT_NAME")
project_id = os.getenv("SAGEMAKER_PROJECT_ID")
sagemaker_execution_role_arn = os.getenv("SAGEMAKER_PIPELINE_ROLE_ARN")
events_role_arn = os.getenv("LAMBDA_ROLE_ARN")

logger = logging.getLogger()

tags = [
    cdk.CfnTag(key="sagemaker:project-id", value=project_id),
    cdk.CfnTag(key="sagemaker:project-name", value=project_name),
]


class BuildModelStack(cdk.Stack):
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        configuration_path: Union[str, Path],
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        eventbridge_role = iam.Role.from_role_arn(
            self, "EventBridgeRole", role_arn=events_role_arn
        )

        if not isinstance(configuration_path, Path):
            configuration_path = Path(configuration_path)

        for k in configuration_path.glob("*.pipeline.json"):
            logger.info(f"Reading configurations file {k.name}")
            pipeline_props = get_pipeline_props(k)

            pipeline_name = f"{project_name}-{pipeline_props['pipeline_name']}"
            pipeline_conf = pipeline_props["pipeline_configuration"]
            try:
                logger.info(f"Generating pipeline definition for {pipeline_name}")
                pipeline_definition = generate_pipeline_definition(
                    role=sagemaker_execution_role_arn,
                    region=os.getenv("AWS_REGION"),
                    default_bucket=project_bucket_name,
                    pipeline_name=pipeline_name,
                    pipeline_conf=pipeline_conf,
                    code_file_path=pipeline_props["code_file_path"],
                )
                logger.info(f"Synthetizing the CFN code for {pipeline_name}")
                sagemaker.CfnPipeline(
                    self,
                    f"SageMakerPipeline-{pipeline_name}",
                    pipeline_name=pipeline_name,
                    pipeline_definition={"PipelineDefinitionBody": pipeline_definition},
                    role_arn=sagemaker_execution_role_arn,
                    tags=tags,
                )
            except:
                logger.exception(f"Failed to create {pipeline_name}")

        codepipeline_arn = ssm.StringParameter.from_string_parameter_name(
            self,
            "ServingPipeline",
            string_parameter_name=f"/sagemaker-{project_name}/{pipeline_construct_id}/CodePipelineARN",
        ).string_value

        pipeline = codepipeline.Pipeline.from_pipeline_arn(
            self, "BuildCodePipeline", pipeline_arn=codepipeline_arn
        )

        features_codepipeline_id = "FeaturesIngestionPipeline"
        features_codepipeline_arn = ssm.StringParameter.from_string_parameter_name(
            self,
            "FeatureIngestionPipeline",
            string_parameter_name=f"/sagemaker-{project_name}/{features_codepipeline_id}/CodePipelineARN",
        ).string_value

        events.Rule(
            self,
            "FeatureIngestionUpdateRule",
            rule_name=f"sagemaker-{project_name}-FeaturesIngestionUpdateRule",
            description="Rule to trigger a new deployment when the Feature Ingestion CodePipeline is executed successfully.",
            event_pattern=events.EventPattern(
                source=["aws.codepipeline"],
                detail_type=["CodePipeline Pipeline Execution State Change"],
                detail={
                    "state": [
                        "SUCCEEDED",
                    ]
                },
                resources=[features_codepipeline_arn],
            ),
            targets=[
                events_targets.CodePipeline(
                    pipeline=pipeline, event_role=eventbridge_role
                )
            ],
        )
