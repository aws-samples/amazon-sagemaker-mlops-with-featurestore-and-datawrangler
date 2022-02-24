import logging
import shutil
from pathlib import Path

from aws_cdk import (
    ArnFormat,
    CfnParameter,
    CfnOutput,
    CustomResource,
    Duration,
    RemovalPolicy,
    Stack,
    Tags,
)
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_servicecatalog_alpha as servicecatalog
from cfn_flip import to_yaml
from constructs import Construct

from infra.mlops_featurestore_construct import MlopsFeaturestoreStack
from infra.utils import generate_template, snake2pascal

logger = logging.getLogger(__name__)


class ServiceCatalogStackNoArtifacts(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        products_launch_role_arn = self.node.try_get_context("MlopsDemo:LaunchRole")
        if products_launch_role_arn is None:
            products_launch_role_arn = self.format_arn(
                region="",
                service="iam",
                resource="role",
                resource_name="service-role/AmazonSageMakerServiceCatalogProductsLaunchRole",
            )
        products_use_role_arn = self.node.try_get_context("MlopsDemo:UseRole")
        if products_use_role_arn is None:
            products_use_role_arn = self.format_arn(
                region="",
                service="iam",
                resource="role",
                resource_name="service-role/AmazonSageMakerServiceCatalogProductsUseRole",
            )

        # Define CloudFormation Parameters
        portfolio_name = CfnParameter(
            self,
            "PortfolioName",
            type="String",
            description="The name of the portfolio",
            default="SageMaker Organization Templates",
            min_length=1,
        )
        portfolio_owner = CfnParameter(
            self,
            "PortfolioOwner",
            type="String",
            description="The owner of the portfolio",
            default="Administrator",
            min_length=1,
            max_length=50,
        )
        product_version = CfnParameter(
            self,
            "ProductVersion",
            type="String",
            description="The product version to deploy",
            default="1.0",
            min_length=1,
        )
        studio_user_role_arn = CfnParameter(
            self,
            "StudioUserRoleARN",
            type="String",
            description="Studio User Role ARN",
            min_length=1,
            allowed_pattern="^arn:aws[a-z\\-]*:iam::\\d{12}:role/?[a-zA-Z_0-9+=,.@\\-_/]+$",
            default=self.node.try_get_context(key="demouserrole"),
        )

        products_launch_role = iam.Role.from_role_arn(
            self,
            "LaunchRole",
            products_launch_role_arn,
        )
        products_use_role = iam.Role.from_role_arn(
            self,
            "ProductsUseRole",
            products_use_role_arn,
        )

        seed_bucket = s3.Bucket(
            self,
            "SeedBucket",
            removal_policy=RemovalPolicy.DESTROY,
        )
        seed_bucket.grant_read(products_launch_role)
        CfnOutput(
            self,
            "SeedBucketNameOutput",
            value=seed_bucket.bucket_name,
            export_name="MLOpsDemo-SeedBucketName-f5e74ee2",
        )

        # Lambda Fn to download the seed code for all the repos and the product template from the GitHub repository
        powertools_lambda_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            "AwsLambdaPowerToolsLayer",
            layer_version_arn=self.format_arn(
                account="017000801446",
                service="lambda",
                resource="layer",
                resource_name="AWSLambdaPowertoolsPython:4",
                arn_format=ArnFormat.COLON_RESOURCE_NAME,
            ),
        )
        git_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            "GitLayer",
            layer_version_arn=self.format_arn(
                account="553035198032",
                service="lambda",
                resource="layer",
                resource_name="git-lambda2:8",
                arn_format=ArnFormat.COLON_RESOURCE_NAME,
            ),
        )

        # Read the lambda fn code into memory to add it inline in the template and avoid creating an artifact
        with Path("lambdas/functions/clone_seeds/lambda_main.py").open() as f:
            seed_lambda_code = f.read()

        seed_lambda = lambda_.Function(
            self,
            "SeedLambda",
            code=lambda_.Code.from_inline(seed_lambda_code),
            handler="index.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            environment=dict(SeedBucket=seed_bucket.bucket_name),
            timeout=Duration.seconds(100),
            layers=[
                powertools_lambda_layer,
                git_layer,
            ],
        )
        seed_bucket.grant_write(seed_lambda)

        seed_paths = [k.as_posix() for k in Path("repos").glob("*") if k.is_dir()]
        demo_path = "demo-workspace"

        code_assets = {
            f"{snake2pascal(k.name)}": dict(
                s3_object_key=k.name + ".zip",
            )
            for k in Path("repos").glob("*")
            if k.is_dir()
        }
        demo_asset = dict(
            s3_object_key="demo-workspace.zip",
        )

        cr = CustomResource(
            self,
            f"CrCloneSeeds",
            service_token=seed_lambda.function_arn,
            properties=dict(
                GitRepository="https://github.com/aws-samples/amazon-sagemaker-mlops-with-featurestore-and-datawrangler",
                Branch="artifact-less_deployment",
                SeedPaths=seed_paths + [demo_path],
                TemplatePath="dist/product.yaml",
            ),
        )
        cloud_formation_template = servicecatalog.CloudFormationTemplate.from_url(
            seed_bucket.virtual_hosted_url_for_object(key="product.yaml")
        )

        product_template = generate_template(
            MlopsFeaturestoreStack,
            "MLOpsCfnStack",
            code_assets=code_assets,
            demo_asset=demo_asset,
        )

        Path("dist").mkdir(exist_ok=True)
        with Path(product_template).open() as j, Path("dist/product.yaml").open(
            "w"
        ) as y:
            y.write(to_yaml(j.read()))

        product_template = shutil.copy2(product_template, "dist/product.yaml")

        # Service Catalog section
        portfolio = servicecatalog.Portfolio(
            self,
            "Portfolio",
            display_name=portfolio_name.value_as_string,
            provider_name=portfolio_owner.value_as_string,
            description="Organization templates for MLOps Demo",
        )

        product = servicecatalog.CloudFormationProduct(
            self,
            "Product",
            owner=portfolio_owner.value_as_string,
            product_name="Amazon SageMaker MLOps Demo",
            product_versions=[
                servicecatalog.CloudFormationProductVersion(
                    cloud_formation_template=cloud_formation_template,
                    product_version_name=product_version.value_as_string,
                )
            ],
            description="Amazon SageMaker MLOps demo project with "
            "Feature Ingestion, "
            "Model Build, "
            "and Deployment pipelines",
        )
        Tags.of(product).add(key="sagemaker:studio-visibility", value="true")
        product.node.add_dependency(cr)

        portfolio.add_product(product)
        studio_user_role = iam.Role.from_role_arn(
            self,
            "execution_role_arn",
            role_arn=studio_user_role_arn.value_as_string,
        )
        portfolio.give_access_to_role(studio_user_role)
        portfolio.set_launch_role(product, products_launch_role)

        launch_role_policies(products_launch_role, self)
        products_launch_role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=[
                    "iam:PutRolePolicy",
                    "iam:DeleteRolePolicy",
                    "iam:getRolePolicy",
                ],
                resources=[products_use_role.role_arn],
            )
        )

        with Path("lambdas/functions/role_name/lambda_main.py").open() as f:
            seed_lambda_code = f.read()

        role_split_lambda = lambda_.Function(
            self,
            "RoleSplitLambda",
            code=lambda_.Code.from_inline(seed_lambda_code),
            handler="index.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(100),
            layers=[
                powertools_lambda_layer,
            ],
        )
        cr2 = CustomResource(
            self,
            f"CrRoleSplitLambda",
            service_token=role_split_lambda.function_arn,
            properties=dict(
                RoleArn=studio_user_role_arn.value_as_string,
            ),
        )

        CfnOutput(
            self,
            "RoleNameOutput",
            value=cr2.get_att_string("RoleName"),
            export_name="MLOpsDemo-RoleName-f5e74ee2",
        )
        CfnOutput(
            self,
            "RoleNameArn",
            value=studio_user_role_arn.value_as_string,
            export_name="MLOpsDemo-RoleArn-f5e74ee2",
        )

        products_launch_role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["iam:PutRolePolicy", "iam:DeleteRolePolicy"],
                resources=[
                    studio_user_role_arn.value_as_string,
                    studio_user_role.role_arn,
                ],
            )
        )





def launch_role_policies(target_role: iam.Role, stack: Stack):
    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "SNS:CreateTopic",
                "SNS:GetTopicAttributes",
                "SNS:DeleteTopic",
                "SNS:ListTagsForResource",
                "SNS:TagResource",
                "SNS:UnTagResource",
                "SNS:Subscribe",
            ],
            resources=[
                stack.format_arn(
                    service="sns",
                    resource="sagemaker-*",
                )
            ],
        )
    )
    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "SNS:Unsubscribe",
            ],
            resources=["*"],
        )
    )
    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=["codebuild:BatchGetProjects"],
            resources=[
                stack.format_arn(
                    service="codebuild",
                    resource="project",
                    resource_name="sagemaker*",
                ),
            ],
        )
    )
    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=["s3:*"],
            resources=[
                stack.format_arn(
                    service="s3",
                    region="",
                    account="",
                    resource="cdktoolkit-stagingbucket-*",
                ),
            ],
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=["ssm:GetParameter"],
            resources=[
                stack.format_arn(
                    service="ssm",
                    resource="parameter",
                    resource_name="cdk-bootstrap/*",
                ),
            ],
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "ssm:PutParameter",
                "ssm:DeleteParameter",
                "ssm:AddTagsToResource",
                "ssm:LabelParameterVersion",
                "ssm:ListTagsForResource",
                "ssm:RemoveTagsFromResource",
                "ssm:DeleteParameter",
                "ssm:DeleteParameters",
            ],
            resources=[
                stack.format_arn(
                    service="ssm",
                    resource="parameter",
                    resource_name="sagemaker*",
                ),
            ],
        )
    )
    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "ssm:DescribeParameters",
            ],
            resources=["*"],
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=["lambda:GetLayerVersion"],
            resources=[
                stack.format_arn(
                    service="lambda",
                    account="017000801446",
                    resource="layer",
                    resource_name="AWSLambdaPowertoolsPython:4",
                    arn_format=ArnFormat.COLON_RESOURCE_NAME,
                ),
            ],
        )
    )
