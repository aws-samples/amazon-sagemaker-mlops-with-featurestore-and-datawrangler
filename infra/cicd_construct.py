from typing import Dict

import aws_cdk as cdk
from aws_cdk import aws_codebuild as codebuild
from aws_cdk import aws_codepipeline as codepipeline
from aws_cdk import aws_codepipeline_actions as codepipeline_actions
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_ssm as ssm
from constructs import Construct

from infra.utils import Repository


class cicd_construct(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        seed_bucket_name: str,
        seed_object_key: str,
        project_bucket: s3.Bucket,
        sm_studio_user_role: iam.Role,
        project_name: str,
        project_id: str,
        sns_topic: sns.Topic,
        roles: Dict[str, iam.Role] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        code_pipeline_role = iam.Role.from_role_arn(
            self, "CP_ROLE", role_arn=roles["code_pipeline_role"], mutable=False
        )
        events_role = iam.Role.from_role_arn(
            self, "E_ROLE", role_arn=roles["events_role"], mutable=False
        )
        code_build_role = iam.Role.from_role_arn(
            self, "CB_ROLE", role_arn=roles["code_build_role"], mutable=False
        )
        cloudformation_role = iam.Role.from_role_arn(
            self,
            "CLOUDFORMATION_ROLE",
            role_arn=roles["cloudformation_role"],
            mutable=False,
        )
        sagemaker_role = iam.Role.from_role_arn(
            self, "SAGEMAKER_ROLE", role_arn=roles["sagemaker_role"], mutable=False
        )
        lambda_role = iam.Role.from_role_arn(
            self, "LAMBDA_ROLE", role_arn=roles["lambda_role"], mutable=False
        )
        api_gateway_role = iam.Role.from_role_arn(
            self, "API_GATEWAY_ROLE", role_arn=roles["api_gateway_role"], mutable=False
        )
        glue_role = iam.Role.from_role_arn(
            self, "GLUE_ROLE", role_arn=roles["glue_role"], mutable=False
        )

        repo = Repository(
            scope,
            f"Sagemaker{construct_id}Repository",
            repository_name=f"sagemaker-{project_name}-{construct_id}",
            code_bucket=seed_bucket_name,
            code_key=seed_object_key,
            tags=[
                cdk.CfnTag(key="sagemaker:project-id", value=project_id),
                cdk.CfnTag(key="sagemaker:project-name", value=project_name),
            ],
        ).repo

        # Define the CodePipeline
        pipeline = codepipeline.Pipeline(
            scope,
            f"{construct_id}CodePipeline",
            artifact_bucket=project_bucket,
            pipeline_name=f"sagemaker-{project_id}-{construct_id}",
            role=code_pipeline_role,
        )

        events.Rule(
            self,
            "CodeCommitRule",
            rule_name=f"sagemaker-{project_name}-codecommit-{construct_id}",
            description="Rule to trigger a build when code is updated in CodeCommit.",
            event_pattern=events.EventPattern(
                source=["aws.codecommit"],
                detail_type=["CodeCommit Repository State Change"],
                detail={
                    "event": ["referenceCreated", "referenceUpdated"],
                    "referenceType": ["branch"],
                    "referenceName": ["main"],
                },
                resources=[repo.repository_arn],
            ),
            targets=[
                targets.CodePipeline(
                    pipeline=pipeline,
                    event_role=events_role,
                )
            ],
        )

        pass_parameter = ssm.StringParameter(
            self,
            f"PipelineARN",
            parameter_name=f"/sagemaker-{project_name}/{construct_id}/CodePipelineARN",
            string_value=pipeline.pipeline_arn,
            simple_name=False,
        )

        env_variables = {
            "PROJECT_BUCKET": codebuild.BuildEnvironmentVariable(
                value=project_bucket.bucket_name
            ),
            "SAGEMAKER_PIPELINE_ROLE_ARN": codebuild.BuildEnvironmentVariable(
                value=sagemaker_role.role_arn,
            ),
            "SAGEMAKER_STUDIO_USER_ROLE_ARN": codebuild.BuildEnvironmentVariable(
                value=sm_studio_user_role.role_arn,
            ),
            "SAGEMAKER_PROJECT_NAME": codebuild.BuildEnvironmentVariable(
                value=project_name
            ),
            "SAGEMAKER_PROJECT_ID": codebuild.BuildEnvironmentVariable(
                value=project_id
            ),
            "CODEPIPELINE_CONSTRUCT_ID": codebuild.BuildEnvironmentVariable(
                value=construct_id
            ),
            "EVENTS_ROLE_ARN": codebuild.BuildEnvironmentVariable(
                value=events_role.role_arn,
            ),
            "LAMBDA_ROLE_ARN": codebuild.BuildEnvironmentVariable(
                value=lambda_role.role_arn,
            ),
            "API_GATEWAY_ROLE_ARN": codebuild.BuildEnvironmentVariable(
                value=api_gateway_role.role_arn,
            ),
            "GLUE_ROLE_ARN": codebuild.BuildEnvironmentVariable(
                value=glue_role.role_arn,
            ),
        }
        source_artifact = codepipeline.Artifact()
        cloud_assembly_artifact = codepipeline.Artifact()

        source_action = codepipeline_actions.CodeCommitSourceAction(
            action_name="CodeCommit",
            repository=repo,
            output=source_artifact,
            trigger=codepipeline_actions.CodeCommitTrigger.NONE,  # Created below
            branch="main",
            event_role=events_role,
            role=code_pipeline_role,
            variables_namespace="SourceVariables",
        )

        # Next is the build action to synthetize the CDK project
        build_project = codebuild.PipelineProject(
            scope,
            f"{construct_id}BuildProject",
            project_name=f"sagemaker-{project_id}-{construct_id}-Synth",
            build_spec=build_spec,
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_5_0,
                environment_variables=env_variables,
                privileged=True,
            ),
            role=code_build_role,
        )

        build_action = codepipeline_actions.CodeBuildAction(
            action_name="CodeBuild",
            project=build_project,
            input=source_artifact,
            outputs=[cloud_assembly_artifact],
            role=code_pipeline_role,
            run_order=1,
        )

        # deploy the synthetized project
        deploy_project = codebuild.PipelineProject(
            scope,
            f"{construct_id}DeployProject",
            project_name=f"sagemaker-{project_id}-{construct_id}Deploy",
            build_spec=deploy_spec,
            cache=codebuild.Cache.local(codebuild.LocalCacheMode.DOCKER_LAYER),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_5_0,
                environment_variables=env_variables,
            ),
            role=cloudformation_role,
        )

        deploy_action = codepipeline_actions.CodeBuildAction(
            action_name="Deploy",
            project=deploy_project,
            input=cloud_assembly_artifact,
            role=code_pipeline_role,
        )

        # Add stages to the pipeline
        pipeline.add_stage(
            stage_name="Source",
            actions=[source_action],
        )
        build_stage = pipeline.add_stage(
            stage_name="Synth",
            actions=[build_action],
        )

        approval_action = codepipeline_actions.ManualApprovalAction(
            # external_entity_link=f"https://{cdk.Aws.REGION}.console.aws.amazon.com/codesuite/"
            #    f"codecommit/repositories/{repo.repository_name}/"
            #    f"commit/{source_action.variables.commit_id}",
            external_entity_link=f"https://{cdk.Aws.REGION}.console.aws.amazon.com/codesuite/codebuild/"
            f"{cdk.Aws.ACCOUNT_ID}/projects/{build_project.project_name}/"
            f"build/{build_action.variable('CODEBUILD_BUILD_ID').replace(':', '%3A')}",
            action_name="ManualApproval",
            notification_topic=sns_topic,
            role=code_pipeline_role,
            additional_information=f"{project_name}-{construct_id} ready to be deployed.\n"
            f"Commit {source_action.variables.commit_id}\n"
            f"{source_action.variables.commit_message}",
            run_order=2,
        )
        build_stage.add_action(approval_action)

        ssm.StringParameter(
            self,
            f"AutoApprovalFlag",
            parameter_name=f"/sagemaker-{project_name}/{construct_id}/AutoApprovalFlag",
            simple_name=False,
            string_value="1",
            allowed_pattern="[0,1]",
        )

        pipeline.add_stage(
            stage_name="Deploy",
            actions=[deploy_action],
        )


build_spec = codebuild.BuildSpec.from_object(
    dict(
        version="0.2",
        phases=dict(
            install={
                "runtime-versions": {
                    "nodejs": "12",
                    "python": "3.8",
                },
                "commands": [
                    "npm install aws-cdk@latest",
                    "npm update",
                    "python -m pip install -U -r requirements.txt",
                ],
            },
            build=dict(
                commands=[
                    "npx cdk diff --path-metadata false",
                ]
            ),
        ),
        artifacts={
            "base-directory": "cdk.out",
            "files": [
                "**/*",
            ],
        },
        env={"exported-variables": ["CODEBUILD_BUILD_ID", "CODEBUILD_BUILD_NUMBER"]},
    )
)

deploy_spec = codebuild.BuildSpec.from_object(
    dict(
        version="0.2",
        phases=dict(
            install={
                "runtime-versions": {
                    "nodejs": "12",
                },
                "commands": [
                    "npm install -g aws-cdk@latest",
                ],
            },
            build=dict(
                commands=[
                    "cdk -a . deploy --all --require-approval=never --verbose",
                ]
            ),
        ),
    )
)
