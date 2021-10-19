import shutil
from pathlib import Path

import boto3
from aws_cdk import aws_codecommit as codecommit
from aws_cdk import core as cdk
from aws_cdk.aws_iam import Role
from aws_cdk.aws_s3_assets import Asset


def generate_template(stack: cdk.Stack, stack_name: str, **kwargs) -> str:
    """Create a CFN template from a stack

    Args:
        stack (cdk.Stack): cdk Stack to synthesize into a CFN template
        stack_name (str): Name to assign to the stack

    Returns:
        [str]: path of the CFN template
    """

    stage = cdk.Stage(cdk.App(), "SynthStage")
    stack(stage, stack_name, **kwargs, synthesizer=cdk.BootstraplessSynthesizer())
    assembly = stage.synth(force=True)
    return assembly.stacks[0].template_full_path


class Repository(object):
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        repository_name: str,
        code_bucket: str,
        code_key: str,
        branch_name: str = "main",
        **kwargs,
    ) -> None:

        repo = codecommit.CfnRepository(
            scope,
            f"{construct_id}-L1",
            repository_name=repository_name,
            code=codecommit.CfnRepository.CodeProperty(
                s3=codecommit.CfnRepository.S3Property(
                    bucket=code_bucket,
                    key=code_key,
                    object_version=None,
                ),
                branch_name=branch_name,
            ),
        )
        repo.apply_removal_policy(
            cdk.RemovalPolicy.DESTROY, apply_to_update_replace_policy=True
        )

        # Reference the newly created repository
        self.repo = codecommit.Repository.from_repository_name(
            scope, f"{construct_id}-L2", repo.attr_name
        )


def code_asset_upload(stack: cdk.Stack, dir_path: Path, read_role: Role):
    # this is a hack, somehow the CDK generated asset is corrupted
    archive = shutil.make_archive(
        base_name=Path("cdk.out") / dir_path.name,
        format="zip",
        root_dir=dir_path,
        base_dir=".",
    )
    asset = Asset(
        stack,
        f"{dir_path.name}CodeSeed",
        path=archive,
    )
    # asset = Asset(
    #     self, f"{dir_path.name}CodeSeed", path=dir_path.as_posix()
    # )
    asset.grant_read(read_role)
    return asset


def get_default_sagemaker_role():
    sm = boto3.client("sagemaker")

    res = sm.list_domains()
    domain_id = res["Domains"][0]["DomainId"]
    res = sm.describe_domain(DomainId=domain_id)
    arn = res["DefaultUserSettings"]["ExecutionRole"]
    return arn


def snake2pascal(test_str: str):
    return test_str.replace("_", " ").title().replace(" ", "")
