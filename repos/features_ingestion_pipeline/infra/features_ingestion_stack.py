import logging
import os
from pathlib import Path
from typing import List, Union
from uuid import uuid4

from aws_cdk import aws_events as events
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sagemaker as sagemaker
from aws_cdk import aws_ssm as ssm
from aws_cdk import core as cdk

from infra.feature_store_utils import get_fg_conf
from infra.sm_pipeline_utils import generate_pipeline_definition, get_pipeline_props

project_bucket_name = os.getenv("PROJECT_BUCKET")
sagemaker_execution_role_arn = os.getenv("SAGEMAKER_PIPELINE_ROLE_ARN")
project_name = os.getenv("SAGEMAKER_PROJECT_NAME")
project_id = os.getenv("SAGEMAKER_PROJECT_ID")
sm_studio_user_role_arn = os.getenv("SAGEMAKER_STUDIO_USER_ROLE_ARN")
events_role_arn = os.getenv("LAMBDA_ROLE_ARN")

logger = logging.getLogger()

tags = [
    cdk.CfnTag(key="sagemaker:project-id", value=project_id),
    cdk.CfnTag(key="sagemaker:project-name", value=project_name),
]


class FeatureIngestionStack(cdk.Stack):
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        configuration_path: Union[str, Path],
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # role = iam.Role(
        #     self,
        #     "FeatureGroupRole",
        #     assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
        #     managed_policies=[
        #         iam.ManagedPolicy.from_aws_managed_policy_name(
        #             "AmazonSageMakerFeatureStoreAccess"
        #         )
        #     ],
        # )

        sm_studio_user_role = iam.Role.from_role_arn(
            self, "SageMakerStudioUserRole", role_arn=sm_studio_user_role_arn
        )
        sagemaker_execution_role = iam.Role.from_role_arn(
            self, "SageMakerExecutionRole", role_arn=sagemaker_execution_role_arn
        )

        # Add FeatureStore access policies
        sagemaker_execution_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonSageMakerFeatureStoreAccess"
            )
        )
        eventbridge_role = iam.Role.from_role_arn(
            self, "EventBridgeRole", role_arn=events_role_arn
        )
        # eventbridge_role = iam.Role(
        #     self,
        #     "EventBridgeRole",
        #     assumed_by=iam.ServicePrincipal("events.amazonaws.com"),
        #     inline_policies=[
        #         iam.PolicyDocument(
        #             statements=[
        #                 iam.PolicyStatement(
        #                     actions=["sagemaker:StartPipelineExecution"],
        #                     resources=[
        #                         f"arn:aws:sagemaker:{self.region}:{self.account}:pipeline/*"
        #                     ],
        #                 )
        #             ]
        #         )
        #     ],
        # )
        # Create the bucket to store the offline store
        offline_bucket = s3.Bucket(
            self,
            "Sagemaker-FeatureStoreOfflineBucket",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            bucket_name=f"sagemaker-{project_id}-fg-{uuid4().hex[:7]}",
        )
        # offline_bucket.grant_read_write(role)

        if not isinstance(configuration_path, Path):
            configuration_path = Path(configuration_path)

        ### Create Feature Groups from configurations files
        for k in configuration_path.glob("*.fg.json"):
            fg_configuration = get_fg_conf(
                file_path=k, bucket_name=offline_bucket.bucket_name
            )
            sagemaker.CfnFeatureGroup(
                self,
                f"FeatureGroup{fg_configuration['feature_group_name']}",
                **fg_configuration,
                role_arn=sagemaker_execution_role_arn,
                tags=tags,
            )

        ### Create SM pipelines from configurations files
        sm_pipelines = {}
        for k in configuration_path.glob("*.pipeline.json"):

            pipeline_props = get_pipeline_props(k)

            pipeline_name = f"{project_name}-{pipeline_props['pipeline_name']}"
            pipeline_conf = pipeline_props["pipeline_configuration"]
            pipeline_definition = generate_pipeline_definition(
                role=sagemaker_execution_role_arn,
                region=os.getenv("AWS_REGION"),
                default_bucket=project_bucket_name,
                pipeline_name=pipeline_name,
                pipeline_conf=pipeline_conf,
                code_file_path=pipeline_props["code_file_path"],
            )

            sm_pipeline = sagemaker.CfnPipeline(
                self,
                f"SageMakerPipeline-{pipeline_name}",
                pipeline_name=pipeline_name,
                pipeline_definition={"PipelineDefinitionBody": pipeline_definition},
                role_arn=sagemaker_execution_role_arn,
                tags=tags,
            )
            sm_pipeline_arn = f"arn:aws:sagemaker:{self.region}:{self.account}:pipeline/{sm_pipeline.pipeline_name.lower()}"

            #### Event bridge scheduling
            rule = events.Rule(
                self,
                f"ScheduledSourceProcessing{pipeline_props['pipeline_name']}",
                rule_name=f"{project_name}-{pipeline_props['pipeline_name']}",
                schedule=events.Schedule.rate(cdk.Duration.hours(12)),
            )
            input_data_uri = ssm.StringParameter(
                self,
                f"{pipeline_props['pipeline_name']}SourceFileUri",
                parameter_name=f"/sagemaker-{project_name}/{pipeline_conf['feature_group_name']}",
                string_value=f"s3://{project_bucket_name}/data/raw/{pipeline_conf['feature_group_name']}.csv",
            )
            input_data_uri.grant_read(sm_studio_user_role)

            self.add_sagemaker_pipeline_target(
                rule.node.default_child,
                event_role_arn=eventbridge_role.role_arn,
                sagemaker_pipeline_arn=sm_pipeline_arn,
                pipeline_parameters={"InputDataUrl": input_data_uri.string_value},
            )

            sm_pipelines = {**sm_pipelines, pipeline_name: sm_pipeline}

    def add_sagemaker_pipeline_target(
        self,
        rule: events.CfnRule,
        event_role_arn: str,
        sagemaker_pipeline_arn: str,
        pipeline_parameters: dict,
    ) -> None:
        """Use events.CfnRule instead of events.Rule to accommodate
        [custom target](https://github.com/aws/aws-cdk/issues/14887)

        Args:
            rule (events.IRule): The event rule to add Target
            event_role (iam.Role): The event role
            sagemaker_pipeline_arn (str): The SageMaker Pipeline ARN
        """

        parameters_list = [
            {"Name": k, "Value": o} for k, o in pipeline_parameters.items()
        ]
        sagemaker_pipeline_target = {
            "Arn": sagemaker_pipeline_arn,
            "Id": "Target0",
            "RoleArn": event_role_arn,
            "SageMakerPipelineParameters": {"PipelineParameterList": parameters_list},
        }
        rule.add_property_override("Targets", [sagemaker_pipeline_target])
