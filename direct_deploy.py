#!/usr/bin/env python3
from pathlib import Path

from aws_cdk import aws_iam as iam
from aws_cdk import core as cdk

from infra.mlops_featurestore_construct import MlopsFeaturestoreConstruct
from infra.utils import code_asset_upload, get_default_sagemaker_role, snake2pascal


class MlopsFeaturestoreStack(cdk.Stack):
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        sm_studio_user_role = iam.Role.from_role_arn(
            self, "execution_role_arn", role_arn=get_default_sagemaker_role()
        )

        products_use_role = iam.Role.from_role_arn(
            self,
            "ProductsUseRole",
            f"arn:{self.partition}:iam::{self.account}:role/"
            "service-role/AmazonSageMakerServiceCatalogProductsUseRole",
        )

        policy = products_use_role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=[
                    "sts:AssumeRole",
                    "iam:PassRole",
                ],
                resources=[
                    products_use_role.role_arn,
                ],
            )
        )

        # Create assets for the seed code of the repositories
        code_assets = {
            f"{snake2pascal(k.name)}": code_asset_upload(
                self, dir_path=k, read_role=products_use_role
            )
            for k in Path("repos").glob("*")
            if k.is_dir()
        }

        demo_asset = code_asset_upload(
            self, dir_path=Path("demo-workspace"), read_role=products_use_role
        )

        mlops_construct = MlopsFeaturestoreConstruct(
            self,
            "MLOpsProject",
            sm_studio_user_role=sm_studio_user_role,
            code_assets=code_assets,
            demo_asset=demo_asset,
            debug_mode=True,
        )
        mlops_construct.node.add_dependency(policy.policy_dependable)


app = cdk.App()

MlopsFeaturestoreStack(
    app,
    "MlopsFeaturestoreStack",
)
app.synth()
