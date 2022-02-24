import os

import aws_cdk as cdk
from aws_cdk import aws_codepipeline as codepipeline
from aws_cdk import aws_codepipeline_actions as codepipeline_actions
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as events_targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_ssm as ssm
from constructs import Construct

project_bucket_name = os.getenv("PROJECT_BUCKET")
execution_role_arn = os.getenv("SAGEMAKER_PIPELINE_ROLE_ARN")
project_name = os.getenv("SAGEMAKER_PROJECT_NAME")
project_id = os.getenv("SAGEMAKER_PROJECT_ID")
pipeline_construct_id = os.getenv("CODEPIPELINE_CONSTRUCT_ID")
events_role_arn = os.getenv("LAMBDA_ROLE_ARN")


class Redeploy(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        model_package_group_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        eventbridge_role = iam.Role.from_role_arn(
            self, "EventBridgeRole", role_arn=events_role_arn
        )

        codepipeline_arn = ssm.StringParameter.from_string_parameter_name(
            self,
            "ServingPipeline",
            string_parameter_name=f"/sagemaker-{project_name}/{pipeline_construct_id}/CodePipelineARN",
        ).string_value

        pipeline = codepipeline.Pipeline.from_pipeline_arn(
            self, "CodePipeline", pipeline_arn=codepipeline_arn
        )

        # Add deploy role to target the code pipeline when model package is approved
        events.Rule(
            self,
            "ModelRegistryRule",
            rule_name=f"{model_package_group_name}-modelregistry",
            description="Rule to trigger a deployment when SageMaker Model registry is updated with a new model package.",
            event_pattern=events.EventPattern(
                source=["aws.sagemaker"],
                detail_type=["SageMaker Model Package State Change"],
                detail={
                    "ModelPackageGroupName": [
                        model_package_group_name,
                    ],
                    "ModelApprovalStatus": [
                        "Approved",
                        "Rejected",
                    ],
                },
            ),
            targets=[
                events_targets.CodePipeline(
                    pipeline=pipeline, event_role=eventbridge_role
                )
            ],
        )
