from pathlib import Path

from aws_cdk import aws_iam as iam
from aws_cdk import aws_servicecatalog as servicecatalog
from aws_cdk import core as cdk

from infra.mlops_featurestore_construct import MlopsFeaturestoreStack
from infra.utils import (
    code_asset_upload,
    generate_template,
    get_default_sagemaker_role,
    snake2pascal,
)

sm_studio_user_role_arn = get_default_sagemaker_role()


class ServiceCatalogStack(cdk.Stack):
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Define CloudFormation Parameters
        portfolio_name = cdk.CfnParameter(
            self,
            "PortfolioName",
            type="String",
            description="The name of the portfolio",
            default="SageMaker Organization Templates",
            min_length=1,
        )
        portfolio_owner = cdk.CfnParameter(
            self,
            "PortfolioOwner",
            type="String",
            description="The owner of the portfolio",
            default="Administrator",
            min_length=1,
            max_length=50,
        )
        product_version = cdk.CfnParameter(
            self,
            "ProductVersion",
            type="String",
            description="The product version to deploy",
            default="1.0",
            min_length=1,
        )

        products_launch_role = iam.Role.from_role_arn(
            self,
            "LaunchRole",
            role_arn=f"arn:{self.partition}:iam::{self.account}:role/"
            "service-role/AmazonSageMakerServiceCatalogProductsLaunchRole",
        )

        products_use_role = iam.Role.from_role_arn(
            self,
            "ProductsUseRole",
            f"arn:{self.partition}:iam::{self.account}:role/"
            "service-role/AmazonSageMakerServiceCatalogProductsUseRole",
        )

        sm_studio_user_role = iam.Role.from_role_arn(
            self,
            "SmExecutionRole",
            sm_studio_user_role_arn,
        )

        # Create assets for the seed code of the repositories
        code_assets = {
            f"{snake2pascal(k.name)}": code_asset_upload(
                self, dir_path=k, read_role=products_launch_role
            )
            for k in Path("repos").glob("*")
            if k.is_dir()
        }

        demo_asset = code_asset_upload(
            self, dir_path=Path("demo-workspace"), read_role=products_launch_role
        )

        product_template = generate_template(
            MlopsFeaturestoreStack,
            "MLOpsCfnStack",
            sm_studio_user_role=sm_studio_user_role,
            code_assets=code_assets,
            demo_asset=demo_asset,
        )

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
                    cloud_formation_template=servicecatalog.CloudFormationTemplate.from_asset(
                        product_template
                    ),
                    product_version_name=product_version.value_as_string,
                )
            ],
            description="Amazon SageMaker Project for a build and deployment pipeline",
        )
        cdk.Tags.of(product).add(key="sagemaker:studio-visibility", value="true")

        portfolio.add_product(product)
        portfolio.give_access_to_role(
            iam.Role.from_role_arn(
                self, "execution_role_arn", role_arn=get_default_sagemaker_role()
            )
        )
        portfolio.set_launch_role(product, products_launch_role)

        launch_role_policies(products_launch_role)
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


def launch_role_policies(target_role: iam.Role):
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
                f"arn:aws:sns:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:sagemaker-*"
            ],
        )
    )
    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=["codebuild:BatchGetProjects"],
            resources=["arn:aws:codebuild:*:*:project/sagemaker*"],
        )
    )
    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=["s3:*"],
            resources=["arn:aws:s3:::cdktoolkit-stagingbucket-*"],
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=["ssm:GetParameter"],
            resources=[
                f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/cdk-bootstrap/*"
            ],
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=[
                "ssm:PutParameter",
                "ssm:DeleteParameter",
                "ssm:AddTagsToResource",
                "ssm:DescribeParameters",
                "ssm:LabelParameterVersion",
                "ssm:ListTagsForResource",
                "ssm:RemoveTagsFromResource",
            ],
            resources=[
                f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/sagemaker*"
            ],
        )
    )

    target_role.add_to_principal_policy(
        iam.PolicyStatement(
            actions=["lambda:GetLayerVersion"],
            resources=[
                f"arn:aws:lambda:{cdk.Aws.REGION}:017000801446:layer:AWSLambdaPowertoolsPython:3"
            ],
        )
    )
