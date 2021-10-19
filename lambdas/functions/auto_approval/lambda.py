import json
import os
from difflib import ndiff

import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.data_classes import SNSEvent
from aws_lambda_powertools.utilities.data_classes.event_source import event_source

logger = Logger()
cp_client = boto3.client("codepipeline")
ssm_client = boto3.client("ssm")

project_name = os.getenv("PROJECT_NAME")
project_id = os.getenv("PROJECT_ID")


@logger.inject_lambda_context()
@event_source(data_class=SNSEvent)
def lambda_handler(event: SNSEvent, context):
    for record in event.records:
        process_sns_message(json.loads(record.sns.message))
    return


def process_sns_message(sns_message: SNSEvent.sns_message):
    token = sns_message["approval"]["token"]
    pipeline = sns_message["approval"]["pipelineName"]
    stage = sns_message["approval"]["stageName"]
    approval_action = sns_message["approval"]["actionName"]

    logger.info(f"Processing approval for {pipeline}")

    pipeline_name = "".join(
        [k[2] for k in ndiff(f"sagemaker-{project_id}-", pipeline) if k[0] == "+"]
    )
    try:
        flag = ssm_client.get_parameter(
            Name=f"/sagemaker-{project_name}/{pipeline_name}/AutoApprovalFlag"
        )["Parameter"]["Value"]
        flag = bool(flag)
    except:
        logger.exception("Failed to read SSM parameter, default to Manual Approval")
        return

    if not flag:
        logger.info(f"Automatic approval for {pipeline} disabled")
        return

    try:
        cp_client.put_approval_result(
            pipelineName=pipeline,
            stageName=stage,
            actionName=approval_action,
            result={
                "summary": "Automatically approved by Lambda.",
                "status": "Approved",
            },
            token=token,
        )
        logger.info(f"Automatic approval for {pipeline} successful")
    except:
        logger.exception("Failed to Automatically approve the pipeline")

    return
