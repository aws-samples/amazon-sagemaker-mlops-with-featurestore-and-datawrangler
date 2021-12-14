import json
import os
import pathlib
from typing import List

import sagemaker
from sagemaker import clarify
from sagemaker.dataset_definition.inputs import (
    AthenaDatasetDefinition,
    DatasetDefinition,
)
from sagemaker.inputs import TransformInput
from sagemaker.model_metrics import MetricsSource, ModelMetrics
from sagemaker.model_monitor import ModelQualityMonitor
from sagemaker.model_monitor.dataset_format import DatasetFormat
from sagemaker.processing import ProcessingInput, ProcessingOutput, Processor
from sagemaker.session import Session
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.workflow.functions import Join
from sagemaker.workflow.parameters import (
    ParameterInteger,
    ParameterString,
    ParameterTypeEnum,
)
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.step_collections import EstimatorTransformer, RegisterModel
from sagemaker.workflow.steps import CacheConfig, ProcessingStep, Step, TrainingStep
from sagemaker.xgboost.estimator import XGBoost

project_name = os.getenv("SAGEMAKER_PROJECT_NAME")
project_id = os.getenv("SAGEMAKER_PROJECT_ID")


def create_pipeline(
    role: str, pipeline_name: str, sagemaker_session: Session = None, **kwargs
) -> Pipeline:

    project_name = os.getenv("SAGEMAKER_PROJECT_NAME", "")
    project_id = os.getenv("SAGEMAKER_PROJECT_ID", "")

    default_bucket = sagemaker_session.default_bucket()
    prefix = kwargs["prefix"]
    model_entry_point = kwargs["model_entry_point"]
    model_package_group_name = kwargs["model_package_group_name"]
    model_package_group_name = f"{project_name}-{model_package_group_name}"

    train_instance_param = ParameterString(
        name="TrainingInstance",
        default_value="ml.m4.xlarge",
    )
    baseline_instance_type = ParameterString(
        name="BaselineInstanceType", default_value="ml.m5.xlarge"
    )
    model_approval_status = ParameterString(
        name="ModelApprovalStatus", default_value="PendingManualApproval"
    )
    train_instance_count = 1
    create_dataset_instance_count = 1
    transformer_instance_count = 1

    ##### Create Dataset
    create_dataset_step = dataset_step(
        role=role,
        sagemaker_session=sagemaker_session,
        instance_count=create_dataset_instance_count,
        **kwargs,
    )

    #### Data Quality Baseline
    data_quality_baseline_step = data_quality_baseline(
        role=role,
        sagemaker_session=sagemaker_session,
        dataset_uri=create_dataset_step.properties.ProcessingOutputConfig.Outputs[
            "baseline"
        ].S3Output.S3Uri,
        instance_type=baseline_instance_type,
        instance_count=create_dataset_instance_count,
        **kwargs,
    )

    # Model training step
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
        hyperparameters=hyperparameters,
        role=role,
        instance_count=train_instance_count,
        instance_type=train_instance_param,
        framework_version="1.0-1",
        sagemaker_session=sagemaker_session,
    )

    train_step = TrainingStep(
        name="ModelTraining",
        estimator=xgb_estimator,
        inputs={
            "train": sagemaker.inputs.TrainingInput(
                s3_data=create_dataset_step.properties.ProcessingOutputConfig.Outputs[
                    "train_data"
                ].S3Output.S3Uri
            )
        },
    )

    transformer = EstimatorTransformer(
        name="TestScoring-",
        estimator=train_step.estimator,
        model_data=train_step.properties.ModelArtifacts.S3ModelArtifacts,
        model_inputs=None,
        instance_type=train_instance_param,
        instance_count=transformer_instance_count,
        transform_inputs=TransformInput(
            data=create_dataset_step.properties.ProcessingOutputConfig.Outputs[
                "test_data"
            ].S3Output.S3Uri,
            content_type="text/csv",
            data_type="S3Prefix",
            split_type="Line",
            input_filter="$[1:]",
            output_filter="$[0, -1]",
            join_source="Input",
        ),
        accept="text/csv",
        assemble_with="Line",
        description="Scoring of test dataset",
        output_path=f"s3://{default_bucket}/{prefix}/test/output",
    )

    ### Model Quality Baseline
    model_quality_baseline_step = model_quality_baseline(
        role=role,
        sagemaker_session=sagemaker_session,
        dataset_uri=f"s3://{default_bucket}/{prefix}/test/output",
        instance_type=baseline_instance_type,
        instance_count=create_dataset_instance_count,
        depends_on=[transformer.steps[-1]],
        **kwargs,
    )

    ### Data bias analysis
    bias_step = bias(
        role=role,
        sagemaker_session=sagemaker_session,
        dataset_uri=create_dataset_step.properties.ProcessingOutputConfig.Outputs[
            "train_data"
        ].S3Output.S3Uri,
        instance_type=baseline_instance_type,
        instance_count=create_dataset_instance_count,
        **kwargs,
    )

    model_metrics = ModelMetrics(
        #         bias=MetricsSource(
        #             content_type="application/json",
        #             s3_uri=bias_step.properties.ProcessingOutputConfig.Outputs["analysis_result"].S3Output.S3Uri,
        #         ),
        model_constraints=MetricsSource(
            content_type="application/json",
            s3_uri=Join(
                on="/",
                values=[
                    model_quality_baseline_step.properties.ProcessingOutputConfig.Outputs[
                        "model_quality"
                    ].S3Output.S3Uri,
                    "constraints.json",
                ],
            ),
        ),
        model_statistics=MetricsSource(
            content_type="application/json",
            s3_uri=Join(
                on="/",
                values=[
                    model_quality_baseline_step.properties.ProcessingOutputConfig.Outputs[
                        "model_quality"
                    ].S3Output.S3Uri,
                    "statistics.json",
                ],
            ),
        ),
        model_data_constraints=MetricsSource(
            content_type="application/json",
            s3_uri=Join(
                on="/",
                values=[
                    data_quality_baseline_step.properties.ProcessingOutputConfig.Outputs[
                        "data_baseline"
                    ].S3Output.S3Uri,
                    "constraints.json",
                ],
            ),
        ),
        model_data_statistics=MetricsSource(
            content_type="application/json",
            s3_uri=Join(
                on="/",
                values=[
                    data_quality_baseline_step.properties.ProcessingOutputConfig.Outputs[
                        "data_baseline"
                    ].S3Output.S3Uri,
                    "statistics.json",
                ],
            ),
        ),
    )
    register_step = RegisterModel(
        name="RegisterModel",
        estimator=train_step.estimator,
        model_data=train_step.properties.ModelArtifacts.S3ModelArtifacts,
        content_types=["text/csv"],
        response_types=["text/csv"],
        inference_instances=["ml.t2.medium", "ml.t2.large", "ml.m5.large"],
        transform_instances=["ml.m5.xlarge"],
        model_package_group_name=model_package_group_name,
        approval_status=model_approval_status,
        model_metrics=model_metrics,
        depends_on=[data_quality_baseline_step, bias_step, model_quality_baseline_step],
        description="Binary classification model based on XGBoost",
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
            bias_step,
            data_quality_baseline_step,
            train_step,
            transformer,
            model_quality_baseline_step,
            register_step,
        ],
        sagemaker_session=sagemaker_session,
    )

    return pipeline


def dataset_step(
    role: str,
    sagemaker_session: Session,
    instance_count: int = 1,
    depends_on: List[Step] = None,
    **kwargs,
 ) -> Step:
    default_bucket = sagemaker_session.default_bucket()
    client = sagemaker_session.boto_session.client("sagemaker")
    customers_fg_name = kwargs["customers_fg_name"]
    claims_fg_name = kwargs["claims_fg_name"]
    script_path = kwargs["create_dataset_script_path"]
    prefix = kwargs["prefix"]
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

    # Create dataset step
    create_dataset_processor = SKLearnProcessor(
        framework_version="0.23-1",
        role=role,
        instance_type="ml.m5.xlarge",
        instance_count=instance_count,
        base_job_name=f"{prefix}/create-dataset",
        sagemaker_session=sagemaker_session,
    )

    training_columns_string = ", ".join(f'"{c}"' for c in training_columns)

    query_string = f"""
    SELECT DISTINCT {training_columns_string}
        FROM "{claims_table}" claims LEFT JOIN "{customers_table}" customers
        ON claims.policy_id = customers.policy_id
    """
    athena_data_path = "/opt/ml/processing/athena"

    data_sources = [
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
    ]

    step = ProcessingStep(
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
        code=script_path,
    )
    return step


def data_quality_baseline(
    role: str,
    sagemaker_session: Session,
    dataset_uri: str,
    instance_type: str = "ml.m5.xlarge",
    instance_count: int = 1,
    depends_on: List[Step] = None,
    **kwargs,
) -> Step:
    prefix = kwargs["prefix"]

    # Get the default model monitor container
    region = sagemaker_session.boto_region_name
    model_monitor_container_uri = sagemaker.image_uris.retrieve(
        framework="model-monitor",
        region=region,
        version="latest",
    )

    # Create the baseline job using
    dataset_format = DatasetFormat.csv(header=True)
    env = {
        "dataset_format": json.dumps(dataset_format),
        "dataset_source": "/opt/ml/processing/input/baseline_dataset_input",
        "output_path": "/opt/ml/processing/output",
        "publish_cloudwatch_metrics": "Disabled",
    }

    monitor_analyzer = Processor(
        image_uri=model_monitor_container_uri,
        role=role,
        instance_count=instance_count,
        instance_type=instance_type,
        base_job_name=f"{prefix}/data-quality-baseline",
        sagemaker_session=sagemaker_session,
        max_runtime_in_seconds=1800,
        env=env,
    )

    step = ProcessingStep(
        name="DataQualityBaseline",
        processor=monitor_analyzer,
        inputs=[
            ProcessingInput(
                source=dataset_uri,
                destination="/opt/ml/processing/input/baseline_dataset_input",
                input_name="baseline_dataset_input",
            ),
        ],
        outputs=[
            ProcessingOutput(
                source="/opt/ml/processing/output",
                output_name="data_baseline",
            ),
        ],
        depends_on=depends_on,
    )
    return step


def model_quality_baseline(
    role: str,
    sagemaker_session: Session,
    dataset_uri: str,
    instance_type: str = "ml.m5.xlarge",
    instance_count: int = 1,
    depends_on: List[Step] = None,
    **kwargs,
) -> Step:
    prefix = kwargs["prefix"]
    env_model_quality = dict(
        dataset_format=json.dumps(DatasetFormat.csv(header=False)),
        publish_cloudwatch_metrics="Disabled",
        analysis_type="MODEL_QUALITY",
        problem_type="BinaryClassification",
        probability_attribute="_c1",
        ground_truth_attribute="_c0",
        probability_threshold_attribute=".1",
        dataset_source="/opt/ml/processing/input/baseline_dataset_input",
        output_path="/opt/ml/processing/output",
    )

    model_quality_analyzer = ModelQualityMonitor(
        role=role,
        instance_count=instance_count,
        instance_type=instance_type,
        base_job_name=f"{prefix}/monitoring",
        sagemaker_session=sagemaker_session,
        max_runtime_in_seconds=1800,
        env=env_model_quality,
    )

    model_quality_processor = Processor(
        image_uri=model_quality_analyzer.image_uri,
        role=role,
        instance_count=instance_count,
        instance_type=instance_type,
        base_job_name=f"{prefix}/model_quality_monitoring",
        sagemaker_session=sagemaker_session,
        max_runtime_in_seconds=1800,
        env=env_model_quality,
    )

    step = ProcessingStep(
        depends_on=depends_on,
        name="ModelQualityBaseline",
        processor=model_quality_processor,
        inputs=[
            ProcessingInput(
                source=dataset_uri,
                destination="/opt/ml/processing/input/baseline_dataset_input",
                input_name="baseline_dataset_input",
            ),
        ],
        outputs=[
            ProcessingOutput(
                source="/opt/ml/processing/output",
                output_name="model_quality",
            ),
        ],
    )
    return step


def bias(
    role: str,
    sagemaker_session: Session,
    dataset_uri: str,
    instance_type: str = "ml.m5.xlarge",
    instance_count: int = 1,
    depends_on: List[Step] = None,
    **kwargs,
) -> Step:
    prefix = kwargs["prefix"]
    default_bucket = sagemaker_session.default_bucket()
    region = sagemaker_session.boto_region_name
    label_name = kwargs["label_name"]

    clarify_processor = clarify.SageMakerClarifyProcessor(
        role=role,
        instance_count=instance_count,
        instance_type=instance_type,
        sagemaker_session=sagemaker_session,
    )

    # Run bias metrics with clarify steps
    pipeline_bias_output_path = f"s3://{default_bucket}/{prefix}/bias/output"

    # clarify configuration
    bias_data_config = clarify.DataConfig(
        s3_data_input_path=dataset_uri,
        s3_output_path=pipeline_bias_output_path,
        label=label_name,
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

    step = ProcessingStep(
        name="BiasAnalysis",
        processor=clarify_processor,
        inputs=[
            sagemaker.processing.ProcessingInput(
                input_name="analysis_config",
                source=f"{clarify_config_dir}/analysis_config.json",
                destination="/opt/ml/processing/input/config",
            ),
            sagemaker.processing.ProcessingInput(
                input_name="dataset",
                source=dataset_uri,
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
        depends_on=depends_on,
    )
    return step
