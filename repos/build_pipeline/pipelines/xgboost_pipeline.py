import json
import os
import pathlib

import boto3
import sagemaker
from sagemaker import clarify
from sagemaker.dataset_definition.inputs import (
    AthenaDatasetDefinition,
    DatasetDefinition,
)
from sagemaker.model_metrics import MetricsSource, ModelMetrics
from sagemaker.model_monitor.dataset_format import DatasetFormat
from sagemaker.processing import ProcessingInput, ProcessingOutput, Processor
from sagemaker.session import Session
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.step_collections import RegisterModel
from sagemaker.workflow.steps import CacheConfig, ProcessingStep, TrainingStep
from sagemaker.xgboost.estimator import XGBoost

project_name = os.getenv("SAGEMAKER_PROJECT_NAME")
project_id = os.getenv("SAGEMAKER_PROJECT_ID")


def create_pipeline(
    role: str, pipeline_name: str, sagemaker_session: Session = None, **kwargs
) -> Pipeline:

    region = sagemaker_session.boto_region_name
    default_bucket = sagemaker_session.default_bucket()
    client = boto3.client("sagemaker")

    customers_fg_name = kwargs["customers_fg_name"]
    claims_fg_name = kwargs["claims_fg_name"]
    create_dataset_script_path = kwargs["create_dataset_script_path"]
    prefix = kwargs["prefix"]
    model_entry_point = kwargs["model_entry_point"]
    model_package_group_name = kwargs["model_package_group_name"]
    model_package_group_name = f"{project_name}-{model_package_group_name}"

    label_name = kwargs["label_name"]
    features_names = kwargs["features_names"]

    training_columns = [label_name] + features_names

    customer_fg = client.describe_feature_group(FeatureGroupName=customers_fg_name)
    claims_fg = client.describe_feature_group(FeatureGroupName=claims_fg_name)
    database_name = customer_fg["OfflineStoreConfig"]["DataCatalogConfig"]["Database"]
    claims_table = claims_fg["OfflineStoreConfig"]["DataCatalogConfig"]["TableName"]
    customers_table = customer_fg["OfflineStoreConfig"]["DataCatalogConfig"][
        "TableName"
    ]
    catalog = customer_fg["OfflineStoreConfig"]["DataCatalogConfig"]["Catalog"]

    train_instance_param = ParameterString(
        name="TrainingInstance",
        default_value="ml.m4.xlarge",
    )

    model_approval_status = ParameterString(
        name="ModelApprovalStatus", default_value="PendingManualApproval"
    )
    baseline_instance_type = ParameterString(
        name="BaselineInstanceType", default_value="ml.m5.xlarge"
    )

    # Create dataset step
    create_dataset_processor = SKLearnProcessor(
        framework_version="0.23-1",
        role=role,
        instance_type="ml.m5.xlarge",
        instance_count=1,
        base_job_name=f"{prefix}/fraud-demo-create-dataset",
        sagemaker_session=sagemaker_session,
    )

    training_columns_string = ", ".join(f'"{c}"' for c in training_columns)

    query_string = f"""
    SELECT DISTINCT {training_columns_string}
        FROM "{claims_table}" claims LEFT JOIN "{customers_table}" customers
        ON claims.policy_id = customers.policy_id
    """
    athena_data_path = "/opt/ml/processing/athena"

    data_sources = []
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
                    output_s3_uri=f"s3://{default_bucket}/{prefix}/athena/data/",
                    output_format="PARQUET",
                ),
            ),
        )
    )

    create_dataset_step = ProcessingStep(
        name="CreateDataset",
        processor=create_dataset_processor,
        inputs=data_sources,
        outputs=[
            ProcessingOutput(
                output_name="train_data", source="/opt/ml/processing/output/train"
            ),
            ProcessingOutput(
                output_name="test_data", source="/opt/ml/processing/output/test"
            ),
            ProcessingOutput(
                output_name="baseline", source="/opt/ml/processing/output/baseline"
            ),
        ],
        job_arguments=[
            "--athena-data",
            athena_data_path,
        ],
        code=create_dataset_script_path,
    )

    # baseline job step
    # Get the default model monitor container
    model_monitor_container_uri = sagemaker.image_uris.retrieve(
        framework="model-monitor",
        region=region,
        version="latest",
    )

    # Create the baseline job using
    dataset_format = DatasetFormat.csv()
    env = {
        "dataset_format": json.dumps(dataset_format),
        "dataset_source": "/opt/ml/processing/input/baseline_dataset_input",
        "output_path": "/opt/ml/processing/output",
        "publish_cloudwatch_metrics": "Disabled",
    }

    monitor_analyzer = Processor(
        image_uri=model_monitor_container_uri,
        role=role,
        instance_count=1,
        instance_type=baseline_instance_type,
        base_job_name=f"{prefix}/monitoring",
        sagemaker_session=sagemaker_session,
        max_runtime_in_seconds=1800,
        env=env,
    )

    baseline_step = ProcessingStep(
        name="BaselineJob",
        processor=monitor_analyzer,
        inputs=[
            ProcessingInput(
                source=create_dataset_step.properties.ProcessingOutputConfig.Outputs[
                    "baseline"
                ].S3Output.S3Uri,
                destination="/opt/ml/processing/input/baseline_dataset_input",
                input_name="baseline_dataset_input",
            ),
        ],
        outputs=[
            ProcessingOutput(
                source="/opt/ml/processing/output",
                output_name="monitoring_output",
            ),
        ],
    )

    # Model training step
    train_instance_count = 1
    training_job_output_path = f"s3://{default_bucket}/{prefix}/training_jobs"
    metric_uri = f"{prefix}/training_jobs/metrics_output/metrics.json"

    hyperparameters = {
        "max_depth": "3",
        "eta": "0.2",
        "objective": "binary:logistic",
        "num_round": "100",
        "bucket": f"{default_bucket}",
        "object": f"{metric_uri}",
    }

    xgb_estimator = XGBoost(
        entry_point=model_entry_point,
        # output_path=training_job_output_path,
        # code_location=training_job_output_path,
        hyperparameters=hyperparameters,
        role=role,
        instance_count=train_instance_count,
        instance_type=train_instance_param,
        framework_version="1.0-1",
        sagemaker_session=sagemaker_session,
    )

    train_step = TrainingStep(
        name="XGBoostTrain",
        estimator=xgb_estimator,
        inputs={
            "train": sagemaker.inputs.TrainingInput(
                s3_data=create_dataset_step.properties.ProcessingOutputConfig.Outputs[
                    "train_data"
                ].S3Output.S3Uri
            )
        },
    )

    # instantiate the Clarify processor
    clarify_processor = clarify.SageMakerClarifyProcessor(
        role=role,
        instance_count=1,
        instance_type="ml.c5.xlarge",
        sagemaker_session=sagemaker_session,
    )

    # Run bias metrics with clarify steps
    pipeline_bias_output_path = (
        f"s3://{default_bucket}/{prefix}/clarify-output/pipeline/bias"
    )

    # clarify configuration
    bias_data_config = clarify.DataConfig(
        s3_data_input_path=create_dataset_step.properties.ProcessingOutputConfig.Outputs[
            "train_data"
        ].S3Output.S3Uri,
        s3_output_path=pipeline_bias_output_path,
        label="fraud",
        dataset_type="text/csv",
    )

    bias_config = clarify.BiasConfig(
        label_values_or_threshold=[0],
        facet_name="customer_gender_female",
        facet_values_or_threshold=[1],
    )

    analysis_config = bias_data_config.get_config()
    analysis_config.update(bias_config.get_config())
    analysis_config["methods"] = {"pre_training_bias": {"methods": "all"}}

    clarify_config_dir = pathlib.Path("config")
    clarify_config_dir.mkdir(exist_ok=True)
    with open(clarify_config_dir / "analysis_config.json", "w") as f:
        json.dump(analysis_config, f)

    clarify_step = ProcessingStep(
        name="ClarifyProcessor",
        processor=clarify_processor,
        inputs=[
            sagemaker.processing.ProcessingInput(
                input_name="analysis_config",
                source=f"{clarify_config_dir}/analysis_config.json",
                destination="/opt/ml/processing/input/config",
            ),
            sagemaker.processing.ProcessingInput(
                input_name="dataset",
                source=create_dataset_step.properties.ProcessingOutputConfig.Outputs[
                    "train_data"
                ].S3Output.S3Uri,
                destination="/opt/ml/processing/input/data",
            ),
        ],
        outputs=[
            sagemaker.processing.ProcessingOutput(
                source="/opt/ml/processing/output/analysis.json",
                destination=pipeline_bias_output_path,
                output_name="analysis_result",
            )
        ],
    )

    # Register Model step
    model_metrics = ModelMetrics(
        model_statistics=MetricsSource(
            s3_uri=f"s3://{default_bucket}/{metric_uri}",
            content_type="application/json",
        )
    )

    register_step = RegisterModel(
        name="RegisterModel",
        estimator=xgb_estimator,
        model_data=train_step.properties.ModelArtifacts.S3ModelArtifacts,
        content_types=["text/csv"],
        response_types=["text/csv"],
        inference_instances=["ml.t2.medium", "ml.t2.large", "ml.m5.large"],
        transform_instances=["ml.m5.xlarge"],
        model_package_group_name=model_package_group_name,
        approval_status=model_approval_status,
        model_metrics=model_metrics,
    )

    # pipeline instance
    pipeline = Pipeline(
        name=pipeline_name,
        parameters=[
            baseline_instance_type,
            train_instance_param,
            model_approval_status,
        ],
        steps=[
            create_dataset_step,
            baseline_step,
            train_step,
            clarify_step,
            register_step,
        ],
        sagemaker_session=sagemaker_session,
    )

    return pipeline
