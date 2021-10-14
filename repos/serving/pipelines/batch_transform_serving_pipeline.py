import os

import boto3
from sagemaker.dataset_definition.inputs import (
    AthenaDatasetDefinition,
    DatasetDefinition,
)
from sagemaker.inputs import TransformInput
from sagemaker.lambda_helper import Lambda
from sagemaker.model import ModelPackage
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.session import Session
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.workflow.callback_step import (
    CallbackOutput,
    CallbackOutputTypeEnum,
    CallbackStep,
)
from sagemaker.workflow.condition_step import ConditionStep, JsonGet
from sagemaker.workflow.conditions import ConditionEquals
from sagemaker.workflow.lambda_step import (
    LambdaOutput,
    LambdaOutputTypeEnum,
    LambdaStep,
)
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.properties import PropertyFile
from sagemaker.workflow.steps import CacheConfig, ProcessingStep, TransformStep


project_name = os.getenv("SAGEMAKER_PROJECT_NAME")
project_id = os.getenv("SAGEMAKER_PROJECT_ID")


def create_pipeline(
    role: str, pipeline_name: str, sagemaker_session: Session = None, **kwargs
) -> Pipeline:

    default_bucket = sagemaker_session.default_bucket()
    client = boto3.client("sagemaker")

    queue_url = kwargs["queue_url"]
    prefix = kwargs["prefix"]

    datafreshness_func_arn = kwargs["datafreshness_func_arn"]
    create_dataset_script_path = kwargs["create_dataset_script_path"]
    customers_fg_name = kwargs["customers_fg_name"]
    claims_fg_name = kwargs["claims_fg_name"]
    features_names = kwargs["features_names"]


    model_package_group_name = kwargs["model_package_group_name"]

    customer_fg = client.describe_feature_group(FeatureGroupName=customers_fg_name)
    claims_fg = client.describe_feature_group(FeatureGroupName=claims_fg_name)
    database_name = customer_fg["OfflineStoreConfig"]["DataCatalogConfig"]["Database"]
    claims_table = claims_fg["OfflineStoreConfig"]["DataCatalogConfig"]["TableName"]
    customers_table = customer_fg["OfflineStoreConfig"]["DataCatalogConfig"][
        "TableName"
    ]
    catalog = customer_fg["OfflineStoreConfig"]["DataCatalogConfig"]["Catalog"]

    model_package_arn = get_model_package_arn(model_package_group_name)
    model = ModelPackage(model_package_arn=model_package_arn, role=role)

    processing_instance_type = ParameterString(
        name="ProcessingInstanceType", default_value="ml.m5.xlarge"
    )

    inference_instance_type = ParameterString(
        name="InferenceInstanceType", default_value="ml.m5.xlarge"
    )

    # Ingest data from offline feature store
    create_dataset_processor = SKLearnProcessor(
        framework_version="0.23-1",
        role=role,
        instance_type="ml.m5.large",
        instance_count=1,
        base_job_name=f"{prefix}/dataset",
        sagemaker_session=sagemaker_session,
    )

    batch_transform_columns_string = "claims.policy_id," + ", ".join(f'"{c}"' for c in features_names)
    query_string = f"""
    SELECT DISTINCT {batch_transform_columns_string}
        FROM "{claims_table}" claims LEFT JOIN "{customers_table}" customers
        ON claims.policy_id = customers.policy_id
    """
    # WHERE claims.fraud is NULL

    athena_data_path = "/opt/ml/processing/athena"
    data_sources = []
    athena_output_s3_uri = f"s3://{default_bucket}/{prefix}/athena/data/"
    data_sources.append(
        ProcessingInput(
            input_name="athena_dataset",
            dataset_definition=DatasetDefinition(
                local_path=athena_data_path,
                data_distribution_type="FullyReplicated",
                athena_dataset_definition=AthenaDatasetDefinition(
                    catalog=catalog,
                    database=database_name,
                    query_string=query_string,
                    output_s3_uri=athena_output_s3_uri,
                    output_format="PARQUET",
                ),
            ),
        )
    )

    destination_s3_key = "CreateDataset-Step/output"

    create_dataset_step = ProcessingStep(
        name="CreateDataset",
        processor=create_dataset_processor,
        inputs=data_sources,
        outputs=[
            ProcessingOutput(
                output_name="batch_transform_data",
                source="/opt/ml/processing/output/dataset",
                destination=f"s3://{default_bucket}/{destination_s3_key}",
            )
        ],
        job_arguments=[
            "--athena-data",
            athena_data_path,
        ],
        code=create_dataset_script_path,
    )

    # ##################################################################
    # 1. LambdaStep: data freshness check
    # ##################################################################
    output_param_1 = LambdaOutput(
        output_name="statusCode", output_type=LambdaOutputTypeEnum.String
    )
    output_param_2 = LambdaOutput(
        output_name="body", output_type=LambdaOutputTypeEnum.String
    )

    step_lambda = LambdaStep(
        name="DatafreshnessCheckLambda",
        lambda_func=Lambda(function_arn=datafreshness_func_arn),
        inputs={
            "bucket_name": default_bucket,
            "key_name": "CreateDataset-Step/output/dataset.csv",
        },
        outputs=[output_param_1, output_param_2],
    )

    step_lambda.add_depends_on([create_dataset_step])
    # ##################################################################
    # 2. TransformStep: Batch Transform
    # ##################################################################
    transformer = model.transformer(
        instance_count=1,
        instance_type=inference_instance_type,
        strategy="SingleRecord",
        output_path=f"s3://{default_bucket}/step_transform/output",
        accept="text/csv",
        assemble_with="Line"
    )
    
    step_transform = TransformStep(
        name="BatchTransform",
        transformer=transformer,
        inputs=TransformInput(
            data=f"s3://{default_bucket}/{destination_s3_key}/dataset.csv",
            content_type="text/csv",
            data_type="S3Prefix", 
            split_type="Line",
            input_filter="$[1:]",
            join_source="Input"
        )
    )

    #####################################
    # 3. Callback Step: Trigger Glue Job
    # ##################################################################
    # Create Callback Step
    callback1_output = CallbackOutput(
        output_name="final_status", output_type=CallbackOutputTypeEnum.String
    )
    step_callback_data = CallbackStep(
        name="GluePrepCallbackStep",
        sqs_queue_url=queue_url,
        inputs={
            "bucket": default_bucket,
            "key_to_process": "step_transform/output/dataset.csv.out"
        },
        outputs=[callback1_output],
    )

    step_callback_data.add_depends_on([step_transform])

    # ##################################################################
    # 4. ConditionStep: Check data freshness
    # ##################################################################

    # TODO: customize data checking logic
    cond_e = ConditionEquals(left=step_lambda.properties.Outputs["body"], right="1")

    step_cond = ConditionStep(
        name="DataFreshCond",
        conditions=[cond_e],
        if_steps=[step_transform, step_callback_data],
        else_steps=[],
    )

    # pipeline instance
    pipeline = Pipeline(
        name=f"{project_name}-{pipeline_name}",
        parameters=[processing_instance_type, inference_instance_type],
        steps=[create_dataset_step, step_lambda, step_cond],
        sagemaker_session=sagemaker_session,
    )
    return pipeline


def get_model_package_arn(model_package_group_name: str):
    client = boto3.client("sagemaker")

    return client.list_model_packages(
        ModelPackageGroupName=model_package_group_name,
        ModelApprovalStatus="Approved",
        SortBy="CreationTime",
        SortOrder="Descending",
    )["ModelPackageSummaryList"][0]["ModelPackageArn"]
