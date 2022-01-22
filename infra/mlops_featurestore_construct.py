from uuid import uuid4

import aws_cdk as cdk
from aws_cdk import Aws, CfnParameter, Duration, Stack, Tags
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as lambda_event_sources
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_assets as s3_assets
from aws_cdk import aws_sns as sns
from constructs import Construct

from infra.cicd_construct import cicd_construct
from infra.utils import Repository

roles_names_list = [
    "events_role",
    "code_pipeline_role",
    "cloudformation_role",
    "code_build_role",
    "glue_role",
    "api_gateway_role",
    "sagemaker_role",
    "lambda_role",
]


class MlopsFeaturestoreStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        code_assets: dict,
        demo_asset: dict,
        sm_studio_user_role_arn: str = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        project_name = cdk.CfnParameter(
            self,
            "SageMakerProjectName",
            type="String",
            description="The name of the SageMaker project.",
            min_length=1,
            max_length=16,
            default="MLOpsDemo",
        )
        project_id = CfnParameter(
            self,
            "SageMakerProjectId",
            type="String",
            min_length=1,
            max_length=16,
            description="Service generated Id of the project.",
            default="mlopsdemo-id",
        )

        sm_studio_user_role_arn = cdk.CfnParameter(
            self,
            "DemoSMSUserRole",
            type="String",
            description="Amazon SageMaker User Execution Role to run the Demo walkthrough.",
            default=sm_studio_user_role_arn,
            allowed_pattern="^arn:aws[a-z\-]*:iam::\d{12}:role/?[a-zA-Z_0-9+=,.@\-_/]+$",
        )

        project_name = project_name.value_as_string
        project_id = project_id.value_as_string

        sm_studio_user_role = iam.Role.from_role_arn(
            self,
            "SMSUserRole",
            role_arn=sm_studio_user_role_arn.value_as_string,
        )

        MlopsFeaturestoreConstruct(
            scope=self,
            construct_id="MLOpsProject",
            sm_studio_user_role=sm_studio_user_role,
            project_name=project_name,
            project_id=project_id,
            code_assets=code_assets,
            demo_asset=demo_asset,
        )


class MlopsFeaturestoreConstruct(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        sm_studio_user_role: iam.Role,
        project_name: str = "MLOpsDemo",
        project_id: str = "mlopsdemo-id",
        code_assets: dict = None,
        demo_asset: s3_assets.Asset = None,
        debug_mode: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        Tags.of(self).add(key="sagemaker:project-id", value=project_id)
        Tags.of(self).add(key="sagemaker:project-name", value=project_name)

        products_use_role = iam.Role.from_role_arn(
            self,
            "ProductsUseRole",
            f"arn:{Aws.PARTITION}:iam::{Aws.ACCOUNT_ID}:role/"
            "service-role/AmazonSageMakerServiceCatalogProductsUseRole",
        )
        policy_result = update_execution_policies(
            products_use_role, project_name=project_name, project_id=project_id
        )

        roles_dict = {k: products_use_role.role_arn for k in roles_names_list}

        # bucket to store configurations, artifacts, etc.
        if debug_mode:
            project_bucket = s3.Bucket(
                self,
                "ProjectBucket",
                auto_delete_objects=debug_mode,
                removal_policy=cdk.RemovalPolicy.DESTROY,
            )
            project_bucket.grant_read(products_use_role)

        else:
            project_bucket = s3.Bucket(
                self,
                "ProjectBucket",
                bucket_name=f"sagemaker-{project_id}-{uuid4().hex[:7]}",
            )
            project_bucket.node.add_dependency(policy_result.policy_dependable)

        cicd_topic = sns.Topic(
            self,
            "CiCdTopic",
            display_name=f"{project_name} CI/CD notifications",
            topic_name=f"sagemaker-{project_id}-cicd-topic",
        )

        cicd_topic.grant_publish(products_use_role)

        with open("lambdas/functions/auto_approval/lambda.py", encoding="utf8") as fp:
            lambda_auto_approve_code = fp.read()
        lambda_approval = lambda_.Function(
            self,
            "AutoApprovalLambda",
            function_name=f"sagemaker-{project_name}-auto-approval",
            code=lambda_.Code.from_inline(lambda_auto_approve_code),
            role=products_use_role,
            handler="index.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=Duration.seconds(3),
            memory_size=128,
            environment={
                "PROJECT_NAME": project_name,
                "PROJECT_ID": project_id,
            },
            layers=[
                lambda_.LayerVersion.from_layer_version_arn(
                    self,
                    "LambdaPowerToolsLayer",
                    layer_version_arn=f"arn:aws:lambda:{cdk.Aws.REGION}:017000801446:layer:AWSLambdaPowertoolsPython:4",
                ),
            ],
        )
        lambda_approval.add_event_source(
            lambda_event_sources.SnsEventSource(
                cicd_topic,
                # filter_policy=
            )
        )

        cicd_dict = {
            name: cicd_construct(
                self,
                construct_id=name,
                seed_bucket_name=o['s3_bucket_name'],
                seed_object_key=o['s3_object_key'],
                project_bucket=project_bucket,
                sm_studio_user_role=sm_studio_user_role,
                project_name=project_name,
                project_id=project_id,
                sns_topic=cicd_topic,
                roles=roles_dict,
            )
            for name, o in code_assets.items()
        }

        if demo_asset is not None:
            Repository(
                self,
                f"sagemaker{construct_id}Repository",
                repository_name=f"sagemaker-{project_name}-Demo",
                code_bucket=demo_asset["s3_bucket_name"],
                code_key=demo_asset["s3_object_key"],
                tags=[
                    cdk.CfnTag(key="sagemaker:project-id", value=project_id),
                    cdk.CfnTag(key="sagemaker:project-name", value=project_name),
                ],
            ).repo


def update_execution_policies(
    target_role: iam.Role,
    project_name: str,
    project_id: str,
):
    """Add necessary policies to the target role

    Args:
        target_role (iam.Role): target role
        project_name (str): SageMaker project name
        project_id (str): SageMaker project ID
    """
    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                # "sts:AssumeRole"
                "iam:PassRole"
            ],
            resources=[
                f"arn:aws:iam::{cdk.Aws.ACCOUNT_ID}:role/cdk*",
            ],
        )
    )

    policy = target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "sts:AssumeRole",
                "iam:PassRole",
            ],
            resources=[
                target_role.role_arn,
            ],
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "cloudformation:DescribeStackEvents",
                "cloudformation:GetTemplate",
                "cloudformation:CreateChangeSet",
                "cloudformation:DescribeChangeSet",
                "cloudformation:ExecuteChangeSet",
                "cloudformation:DeleteChangeSet",
                "cloudformation:DescribeStacks",
                "cloudformation:DeleteStack",
            ],
            resources=[
                f"arn:aws:cloudformation:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stack/{project_name}*/*",
            ],
        )
    )
    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "cloudformation:DescribeStackEvents",
                "cloudformation:GetTemplate",
                "cloudformation:DescribeStacks",
            ],
            resources=[
                f"arn:aws:cloudformation:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:stack/CDKToolkit/*",
            ],
        )
    )
    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "ssm:GetParameter",
            ],
            resources=[
                f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/cdk-bootstrap/*",
                f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/sagemaker-{project_name}*",
            ],
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=["*"],
            resources=["*"],
            conditions={
                "ForAnyValue:StringEquals": {
                    "aws:CalledVia": ["cloudformation.amazonaws.com"]
                }
            },
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
            ],
            resources=[
                f"arn:aws:logs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:log-group:/aws/codebuild/sagemaker-{project_id}*",
                f"arn:aws:logs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:/aws/codebuild/sagemaker-{project_id}*:*",
            ],
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "codebuild:CreateReportGroup",
                "codebuild:CreateReport",
                "codebuild:UpdateReport",
                "codebuild:BatchPutTestCases",
                "codebuild:BatchPutCodeCoverages",
            ],
            resources=[
                f"arn:aws:codebuild:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:report-group/sagemaker-{project_id}*",
            ],
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "codepipeline:PutApprovalResult",
            ],
            resources=[
                f"arn:aws:codepipeline:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:sagemaker-{project_id}*",
            ],
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "codebuild:BatchGetBuilds",
                "codebuild:StartBuild",
                "codebuild:StopBuild",
            ],
            resources=[
                f"arn:aws:codebuild:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:project/sagemaker-{project_id}*",
            ],
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "glue:SearchTables",
                "glue:BatchCreatePartition",
                "athena:StartQueryExecution",
                "glue:CreateTable",
                "glue:GetTables",
                "glue:GetTableVersions",
                "glue:GetPartitions",
                "glue:BatchDeletePartition",
                "glue:UpdateTable",
                "glue:DeleteTableVersion",
                "glue:BatchGetPartition",
                "glue:DeleteTable",
                "cloudformation:DescribeStacks",
                "glue:GetTable",
                "glue:GetDatabase",
                "glue:GetPartition",
                "glue:GetTableVersion",
                "glue:CreateDatabase",
                "glue:BatchDeleteTableVersion",
                "athena:GetQueryExecution",
                "glue:BatchDeleteTable",
                "glue:CreatePartition",
                "glue:DeletePartition",
                "glue:UpdatePartition",
            ],
            resources=[
                "arn:aws:glue:*:*:catalog",
                "arn:aws:glue:*:*:database/default",
                "arn:aws:glue:*:*:database/global_temp",
                "arn:aws:glue:*:*:database/sagemaker*",
                "arn:aws:glue:*:*:table/sagemaker*",
                "arn:aws:glue:*:*:tableVersion/sagemaker*",
                f"arn:aws:athena:*:{cdk.Aws.ACCOUNT_ID}:workgroup/*",
            ],
        )
    )
    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=["glue:StartJobRun"],
            resources=[
                f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:job/sagemaker-*"
            ],
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=["glue:GetJobRun", "glue:GetJobRuns", "glue:GetJobs"],
            resources=[f"*"],
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "dynamodb:BatchGetItem",
                "dynamodb:GetRecords",
                "dynamodb:GetShardIterator",
                "dynamodb:Query",
                "dynamodb:GetItem",
                "dynamodb:Scan",
                "dynamodb:ConditionCheckItem",
                "dynamodb:DescribeTable",
            ],
            resources=[
                f"arn:aws:dynamodb:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/sagemaker-{project_id}*"
            ],
        )
    )

    return policy
