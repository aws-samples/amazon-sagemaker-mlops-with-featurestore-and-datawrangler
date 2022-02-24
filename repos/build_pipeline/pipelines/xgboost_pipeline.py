from typing import Dict

import sagemaker
from sagemaker.clarify import BiasConfig, DataConfig
from sagemaker.dataset_definition.inputs import (
    AthenaDatasetDefinition,
    DatasetDefinition,
)
from sagemaker.drift_check_baselines import DriftCheckBaselines
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.inputs import TransformInput
from sagemaker.lambda_helper import Lambda
from sagemaker.model_metrics import MetricsSource, ModelMetrics
from sagemaker.model_monitor.dataset_format import DatasetFormat
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.workflow.check_job_config import CheckJobConfig
from sagemaker.workflow.clarify_check_step import ClarifyCheckStep, DataBiasCheckConfig
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.conditions import ConditionGreaterThanOrEqualTo
from sagemaker.workflow.execution_variables import ExecutionVariables
from sagemaker.workflow.functions import Join
from sagemaker.workflow.lambda_step import (
    LambdaOutput,
    LambdaOutputTypeEnum,
    LambdaStep,
)
from sagemaker.workflow.parameters import (
    ParameterFloat,
    ParameterInteger,
    ParameterString,
)
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.quality_check_step import (
    DataQualityCheckConfig,
    ModelQualityCheckConfig,
    QualityCheckStep,
)
from sagemaker.workflow.step_collections import EstimatorTransformer, RegisterModel
from sagemaker.workflow.steps import CacheConfig, ProcessingStep, Step, TrainingStep
from sagemaker.xgboost.estimator import XGBoost


def get_pipeline(
    role: str,
    pipeline_name: str,
    sagemaker_session: sagemaker.Session = None,
    **kwargs,
) -> Pipeline:
    cache_config = CacheConfig(enable_caching=True, expire_after="PT1H")
    create_dataset_instance_count = 1
    transformer_instance_count = 1

    default_bucket = sagemaker_session.default_bucket()
    prefix = kwargs["prefix"]
    model_package_group_name = kwargs["model_package_group_name"]

    # Pipeline parameters
    train_instance_count = ParameterInteger(
        name="TrainingInstanceCount",
        default_value=1,
    )
    train_instance_type = ParameterString(
        name="TrainingInstance",
        default_value="ml.m4.xlarge",
    )

    baseline_instance_type = ParameterString(
        name="BaselineInstanceType",
        default_value="ml.c5.xlarge",
    )
    baseline_instance_count = ParameterInteger(
        name="BaselineInstanceCount",
        default_value=1,
    )
    model_threshold_auc = ParameterFloat(
        name="ModelMinAcceptableAUC",
        default_value=0.75,
    )
    model_approval_status = ParameterString(
        name="ModelApprovalStatus",
        default_value="PendingManualApproval",
        enum_values=[
            "PendingManualApproval",
            "Approved",
        ],
    )

    check_job_config = CheckJobConfig(
        role=role,
        instance_count=baseline_instance_count,
        instance_type=baseline_instance_type,
        volume_size_in_gb=120,
        sagemaker_session=sagemaker_session,
    )

    ##### Create Dataset
    create_dataset_step = get_dataset_step(
        role=role,
        sagemaker_session=sagemaker_session,
        instance_count=create_dataset_instance_count,
        cache_config=cache_config,
        **kwargs,
    )

    #### Data Quality Baseline
    data_quality_baseline_step = get_data_quality_step(
        role=role,
        sagemaker_session=sagemaker_session,
        dataset_uri=create_dataset_step.properties.ProcessingOutputConfig.Outputs[
            "baseline"
        ].S3Output.S3Uri,
        check_job_config=check_job_config,
        cache_config=cache_config,
        **kwargs,
    )

    # Model training step
    training_step = get_model_training_step(
        role=role,
        sagemaker_session=sagemaker_session,
        dataset_uri=create_dataset_step.properties.ProcessingOutputConfig.Outputs[
            "train_data"
        ].S3Output.S3Uri,
        instance_count=train_instance_count,
        instance_type=train_instance_type,
        cache_config=cache_config,
        **kwargs,
    )

    transformer = EstimatorTransformer(
        name="TestScoring-",
        estimator=training_step.estimator,
        model_data=training_step.properties.ModelArtifacts.S3ModelArtifacts,
        model_inputs=None,
        instance_type=train_instance_type,
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
        output_path=Join(
            on="/",
            values=[
                "s3:/",
                default_bucket,
                prefix,
                ExecutionVariables.PIPELINE_EXECUTION_ID,
                "test_step",
                "output",
            ],
        ),
    )

    ### Model Quality Baseline
    model_quality_baseline_step = get_model_quality_step(
        role=role,
        sagemaker_session=sagemaker_session,
        dataset_uri=transformer.steps[-1].properties.TransformOutput.S3OutputPath,
        check_job_config=check_job_config,
        **kwargs,
    )

    ### Data bias analysis
    bias_step = get_data_bias_step(
        role=role,
        sagemaker_session=sagemaker_session,
        dataset_uri=create_dataset_step.properties.ProcessingOutputConfig.Outputs[
            "train_data"
        ].S3Output.S3Uri,
        check_job_config=check_job_config,
        **kwargs,
    )

    model_metrics = ModelMetrics(
        model_data_statistics=MetricsSource(
            s3_uri=data_quality_baseline_step.properties.CalculatedBaselineStatistics,
            content_type="application/json",
        ),
        model_data_constraints=MetricsSource(
            s3_uri=data_quality_baseline_step.properties.CalculatedBaselineConstraints,
            content_type="application/json",
        ),
        bias_pre_training=MetricsSource(
            s3_uri=bias_step.properties.CalculatedBaselineConstraints,
            content_type="application/json",
        ),
        model_statistics=MetricsSource(
            s3_uri=model_quality_baseline_step.properties.CalculatedBaselineStatistics,
            content_type="application/json",
        ),
        model_constraints=MetricsSource(
            s3_uri=model_quality_baseline_step.properties.CalculatedBaselineConstraints,
            content_type="application/json",
        ),
        bias=MetricsSource(
            s3_uri=bias_step.properties.CalculatedBaselineConstraints,
            content_type="application/json",
        ),
    )

    drift_check_baselines = DriftCheckBaselines(
        model_data_statistics=MetricsSource(
            s3_uri=data_quality_baseline_step.properties.BaselineUsedForDriftCheckStatistics,
            content_type="application/json",
        ),
        model_data_constraints=MetricsSource(
            s3_uri=data_quality_baseline_step.properties.BaselineUsedForDriftCheckConstraints,
            content_type="application/json",
        ),
        bias_pre_training_constraints=MetricsSource(
            s3_uri=bias_step.properties.BaselineUsedForDriftCheckConstraints,
            content_type="application/json",
        ),
        model_statistics=MetricsSource(
            s3_uri=model_quality_baseline_step.properties.BaselineUsedForDriftCheckStatistics,
            content_type="application/json",
        ),
        model_constraints=MetricsSource(
            s3_uri=model_quality_baseline_step.properties.BaselineUsedForDriftCheckConstraints,
            content_type="application/json",
        ),
    )

    register_step = RegisterModel(
        name="RegisterModel",
        estimator=training_step.estimator,
        model_data=training_step.properties.ModelArtifacts.S3ModelArtifacts,
        content_types=["text/csv"],
        response_types=["text/csv"],
        inference_instances=["ml.t2.medium", "ml.t2.large", "ml.m5.large"],
        transform_instances=["ml.m5.xlarge"],
        model_package_group_name=model_package_group_name,
        approval_status=model_approval_status,
        model_metrics=model_metrics,
        drift_check_baselines=drift_check_baselines,
        description="Binary classification model based on XGBoost",
    )

    lambda_step = get_lambda_step(
        sagemaker_session=sagemaker_session,
        function_arn=kwargs["metric_extraction_lambda_arn"],
        model_quality_report_uri=model_quality_baseline_step.properties.CalculatedBaselineStatistics,
        metric_name="auc",
    )

    cond_lte = ConditionGreaterThanOrEqualTo(
        left=lambda_step.properties.Outputs["metric_value"],
        right=model_threshold_auc,
    )

    step_cond = ConditionStep(
        name="CheckAUC",
        conditions=[cond_lte],
        if_steps=[register_step],
        else_steps=[],
    )

    # pipeline instance
    pipeline = Pipeline(
        name=pipeline_name,
        parameters=[
            baseline_instance_type,
            baseline_instance_count,
            train_instance_type,
            train_instance_count,
            model_approval_status,
            model_threshold_auc,
        ],
        steps=[
            create_dataset_step,
            bias_step,
            data_quality_baseline_step,
            training_step,
            transformer,
            model_quality_baseline_step,
            lambda_step,
            step_cond,
        ],
        sagemaker_session=sagemaker_session,
    )

    return pipeline


def get_model_training_step(
    role: str,
    sagemaker_session: sagemaker.Session,
    dataset_uri: str,
    instance_count: int,
    instance_type: str,
    cache_config: CacheConfig = None,
    **kwargs,
):
    default_bucket = sagemaker_session.default_bucket()
    prefix = kwargs["prefix"]
    model_entry_point = kwargs["model_training_script_path"]

    metric_uri = f"{prefix}/training_jobs/metrics_output/metrics.json"
    hyperparameters = {
        "max_depth": "3",
        "eta": "0.2",
        "objective": "binary:logistic",
        "num_round": "100",
        "bucket": f"{default_bucket}",
        "object": f"{metric_uri}",
    }
    estimator = XGBoost(
        entry_point=model_entry_point,
        hyperparameters=hyperparameters,
        role=role,
        instance_count=instance_count,
        instance_type=instance_type,
        framework_version="1.0-1",
        sagemaker_session=sagemaker_session,
    )

    train_step = TrainingStep(
        name="ModelTraining",
        estimator=estimator,
        inputs={"train": sagemaker.inputs.TrainingInput(s3_data=dataset_uri)},
        cache_config=cache_config,
    )

    return train_step


def get_lambda_step(
    sagemaker_session: sagemaker.Session,
    function_arn: str,
    model_quality_report_uri: str,
    metric_name: str,
) -> Step:
    output_param_1 = LambdaOutput(
        output_name="statusCode",
        output_type=LambdaOutputTypeEnum.String,
    )
    output_param_2 = LambdaOutput(
        output_name="body",
        output_type=LambdaOutputTypeEnum.String,
    )
    output_param_3 = LambdaOutput(
        output_name="metric_value",
        output_type=LambdaOutputTypeEnum.String,
    )

    step = LambdaStep(
        name="LambdaExtractMetrics",
        lambda_func=Lambda(function_arn=function_arn, session=sagemaker_session),
        inputs={
            "model_quality_report_uri": model_quality_report_uri,
            "metric_name": metric_name,
        },
        outputs=[output_param_1, output_param_2, output_param_3],
    )
    return step


def get_data_bias_step(
    sagemaker_session: sagemaker.Session,
    dataset_uri: str,
    check_job_config: CheckJobConfig,
    cache_config: CacheConfig = None,
    **kwargs,
) -> Step:
    prefix = kwargs["prefix"]
    default_bucket = sagemaker_session.default_bucket()
    label_name = kwargs["label_name"]

    data_bias_analysis_cfg_output_path = (
        f"s3://{default_bucket}/{prefix}/databiascheckstep/analysis_cfg"
    )

    data_bias_data_config = DataConfig(
        s3_data_input_path=dataset_uri,
        s3_output_path=Join(
            on="/",
            values=[
                "s3:/",
                default_bucket,
                prefix,
                ExecutionVariables.PIPELINE_EXECUTION_ID,
                "databiascheckstep",
            ],
        ),
        label=label_name,
        dataset_type="text/csv",
        s3_analysis_config_output_path=data_bias_analysis_cfg_output_path,
    )

    data_bias_config = BiasConfig(
        label_values_or_threshold=[0],
        facet_name="customer_gender_female",
        facet_values_or_threshold=[1],
    )

    data_bias_check_config = DataBiasCheckConfig(
        data_config=data_bias_data_config,
        data_bias_config=data_bias_config,
    )

    data_bias_check_step = ClarifyCheckStep(
        name="DataBiasCheckStep",
        skip_check=True,
        clarify_check_config=data_bias_check_config,
        check_job_config=check_job_config,
        register_new_baseline=True,
        cache_config=cache_config,
    )
    return data_bias_check_step


def get_model_quality_step(
    sagemaker_session: sagemaker.Session,
    dataset_uri: str,
    check_job_config: CheckJobConfig,
    cache_config: CacheConfig = None,
    **kwargs,
) -> Step:

    prefix = kwargs["prefix"]
    default_bucket = sagemaker_session.default_bucket()

    model_quality_check_config = ModelQualityCheckConfig(
        baseline_dataset=dataset_uri,
        dataset_format=DatasetFormat.csv(header=False),
        output_s3_uri=Join(
            on="/",
            values=[
                "s3:/",
                default_bucket,
                prefix,
                ExecutionVariables.PIPELINE_EXECUTION_ID,
                "modelqualitycheckstep",
            ],
        ),
        problem_type="BinaryClassification",
        probability_attribute="_c1",
        ground_truth_attribute="_c0",
        probability_threshold_attribute=".1",
    )

    model_quality_check_step = QualityCheckStep(
        name="ModelQualityCheckStep",
        skip_check=True,
        register_new_baseline=True,
        quality_check_config=model_quality_check_config,
        check_job_config=check_job_config,
        cache_config=cache_config,
    )
    return model_quality_check_step


def get_data_quality_step(
    sagemaker_session: sagemaker.Session,
    dataset_uri: str,
    check_job_config: CheckJobConfig,
    cache_config: CacheConfig = None,
    **kwargs,
) -> Step:

    prefix = kwargs["prefix"]
    default_bucket = sagemaker_session.default_bucket()

    data_quality_check_config = DataQualityCheckConfig(
        baseline_dataset=dataset_uri,
        dataset_format=DatasetFormat.csv(header=True, output_columns_position="START"),
        output_s3_uri=Join(
            on="/",
            values=[
                "s3:/",
                default_bucket,
                prefix,
                ExecutionVariables.PIPELINE_EXECUTION_ID,
                "dataqualitycheckstep",
            ],
        ),
    )

    data_quality_check_step = QualityCheckStep(
        name="DataQualityCheckStep",
        skip_check=True,
        register_new_baseline=True,
        quality_check_config=data_quality_check_config,
        check_job_config=check_job_config,
        cache_config=cache_config,
    )

    return data_quality_check_step


def get_dataset_step(
    role: str,
    sagemaker_session: sagemaker.Session,
    instance_count: int = 1,
    cache_config: CacheConfig = None,
    **kwargs,
) -> Step:
    default_bucket = sagemaker_session.default_bucket()
    script_path = kwargs["create_dataset_script_path"]
    prefix = kwargs["prefix"]

    athena_data_path = "/opt/ml/processing/athena"

    # Create dataset step
    create_dataset_processor = SKLearnProcessor(
        framework_version="0.23-1",
        role=role,
        instance_type="ml.m5.xlarge",
        instance_count=instance_count,
        base_job_name=f"{prefix}/create-dataset",
        sagemaker_session=sagemaker_session,
    )

    data_sources = [
        ProcessingInput(
            input_name="athena_dataset",
            dataset_definition=DatasetDefinition(
                local_path=athena_data_path,
                data_distribution_type="FullyReplicated",
                athena_dataset_definition=AthenaDatasetDefinition(
                    **generate_query(kwargs, sagemaker_session=sagemaker_session),
                    output_s3_uri=Join(
                        on="/",
                        values=[
                            "s3:/",
                            default_bucket,
                            prefix,
                            ExecutionVariables.PIPELINE_EXECUTION_ID,
                            "raw_dataset",
                        ],
                    ),
                    output_format="PARQUET",
                ),
            ),
        )
    ]

    step = ProcessingStep(
        name="CreateDataset",
        processor=create_dataset_processor,
        cache_config=cache_config,
        inputs=data_sources,
        outputs=[
            ProcessingOutput(
                output_name="train_data",
                source="/opt/ml/processing/output/train",
                destination=Join(
                    on="/",
                    values=[
                        "s3:/",
                        default_bucket,
                        prefix,
                        ExecutionVariables.PIPELINE_EXECUTION_ID,
                        "train_dataset",
                    ],
                ),
            ),
            ProcessingOutput(
                output_name="test_data",
                source="/opt/ml/processing/output/test",
                destination=Join(
                    on="/",
                    values=[
                        "s3:/",
                        default_bucket,
                        prefix,
                        ExecutionVariables.PIPELINE_EXECUTION_ID,
                        "test_dataset",
                    ],
                ),
            ),
            ProcessingOutput(
                output_name="baseline",
                source="/opt/ml/processing/output/baseline",
                destination=Join(
                    on="/",
                    values=[
                        "s3:/",
                        default_bucket,
                        prefix,
                        ExecutionVariables.PIPELINE_EXECUTION_ID,
                        "baseline_dataset",
                    ],
                ),
            ),
        ],
        job_arguments=[
            "--athena-data",
            athena_data_path,
        ],
        code=script_path,
    )
    return step


def generate_query(dataset_dict: Dict, sagemaker_session: sagemaker.Session):
    customer_fg_info = get_fg_info(
        dataset_dict["customers_fg_name"],
        sagemaker_session=sagemaker_session,
    )
    claims_fg_info = get_fg_info(
        dataset_dict["claims_fg_name"],
        sagemaker_session=sagemaker_session,
    )

    label_name = dataset_dict["label_name"]
    features_names = dataset_dict["features_names"]
    training_columns = [label_name] + features_names
    training_columns_string = ", ".join(f'"{c}"' for c in training_columns)

    query_string = f"""SELECT DISTINCT {training_columns_string}
        FROM "{claims_fg_info.table_name}" claims LEFT JOIN "{customer_fg_info.table_name}" customers
        ON claims.policy_id = customers.policy_id
    """
    return dict(
        catalog=claims_fg_info.catalog,
        database=claims_fg_info.database,
        query_string=query_string,
    )


def get_fg_info(fg_name: str, sagemaker_session: sagemaker.Session):
    boto_session = sagemaker_session.boto_session
    featurestore_runtime = sagemaker_session.sagemaker_featurestore_runtime_client
    feature_store_session = sagemaker.Session(
        boto_session=boto_session,
        sagemaker_client=sagemaker_session.sagemaker_client,
        sagemaker_featurestore_runtime_client=featurestore_runtime,
    )
    fg = FeatureGroup(name=fg_name, sagemaker_session=feature_store_session)
    return fg.athena_query()
