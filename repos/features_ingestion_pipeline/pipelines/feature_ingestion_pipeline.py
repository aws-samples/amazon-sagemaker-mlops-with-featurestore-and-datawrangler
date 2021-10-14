import json

from sagemaker.processing import (
    FeatureStoreOutput,
    ProcessingInput,
    ProcessingJob,
    ProcessingOutput,
    Processor,
)
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.wrangler.processing import DataWranglerProcessor

from .parse_flow import FlowFile

def create_pipeline(role, pipeline_name, sagemaker_session=None, **kwarg)-> Pipeline:
    """[summary]


    Args:
        role ([type]): [description]
        pipeline_name ([type]): [description]
        sagemaker_session ([type], optional): [description]. Defaults to None.

    Returns:
        Pipeline: [description]
    """
    flow_file_path = kwarg["flow_file_path"]
    feature_group_name = kwarg["feature_group_name"]

    flow_file = FlowFile(flow_file_path)

    instance_count = ParameterInteger(name="InstanceCount", default_value=1)
    instance_type = ParameterString(name="InstanceType", default_value="ml.m5.4xlarge")
    input_data_url = ParameterString(name="InputDataUrl")

    output_content_type = "CSV"
    output_config = {flow_file.output_name: {"content_type": output_content_type}}
    job_argument = [f"--output-config '{json.dumps(output_config)}'"]

    data_sources = [
        ProcessingInput(
            input_name=flow_file.input_name,
            source=input_data_url,
            destination=f"/opt/ml/processing/{flow_file.input_name}",
        )
    ]

    outputs = [
        ProcessingOutput(
            output_name=flow_file.output_name,
            app_managed=True,
            feature_store_output=FeatureStoreOutput(
                feature_group_name=feature_group_name
            ),
        )
    ]

    data_wrangler_processor = DataWranglerProcessor(
        role=role,
        data_wrangler_flow_source=flow_file_path,
        instance_count=instance_count,
        instance_type=instance_type,
        sagemaker_session=sagemaker_session,
    )

    data_wrangler_step = ProcessingStep(
        name="data-wrangler-step",
        processor=data_wrangler_processor,
        inputs=data_sources,
        outputs=outputs,
        job_arguments=job_argument,
    )

    pipeline = Pipeline(
        name=pipeline_name,
        parameters=[instance_count, instance_type, input_data_url],
        steps=[data_wrangler_step],
        sagemaker_session=sagemaker_session,
    )

    return pipeline
