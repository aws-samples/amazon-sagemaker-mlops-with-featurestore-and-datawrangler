import logging
import os

import aws_cdk as cdk
from aws_cdk import aws_apigateway as apigateway
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_python_alpha as lambda_python
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sagemaker as sagemaker
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_ssm as ssm
from constructs import Construct

from infra.dynamodb_construct import GlueDynamoDb
from infra.sm_pipeline_utils import generate_pipeline_definition

logger = logging.getLogger()

project_bucket_name = os.getenv("PROJECT_BUCKET")
project_name = os.getenv("SAGEMAKER_PROJECT_NAME")
project_id = os.getenv("SAGEMAKER_PROJECT_ID")
events_role_arn = os.getenv("LAMBDA_ROLE_ARN")
glue_role_arn = os.getenv("GLUE_ROLE_ARN")
api_gateway_role_arn = os.getenv("API_GATEWAY_ROLE_ARN")
lambda_role_arn = os.getenv("LAMBDA_ROLE_ARN")
execution_role_arn = os.getenv("SAGEMAKER_PIPELINE_ROLE_ARN")

tags = [
    cdk.CfnTag(key="sagemaker:project-id", value=project_id),
    cdk.CfnTag(key="sagemaker:project-name", value=project_name),
]
class BatchTransform(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        sagemaker_execution_role: iam.Role,
        project_bucket: s3.Bucket,
        model_package_group_name: str,
        pipeline_props: dict,
        features_names: dict,
        api_gw=apigateway.RestApi,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)
        lambda_role = iam.Role.from_role_arn(
            self, "LambdaRole", role_arn=lambda_role_arn
        )


        pipeline_name = f"{project_name}-{pipeline_props['pipeline_name']}"

        logger.info("Defining Data Quality SNS Topic")
        topic = sns.Topic(
            self,
            "BatchTransformPipeline",
            display_name=f"{pipeline_name}-Topic",
            topic_name=f"{pipeline_name}-Topic",
        )

        logger.info("Defining callback SQS Queue")
        callback_queue = sqs.Queue(
            scope=self,
            id=f"{pipeline_name}-Queue",
            queue_name=f"{pipeline_name}-Queue",
            visibility_timeout=cdk.Duration.minutes(15),
        )
        self.callback_queue = callback_queue

        # Create lambda function to check data freshness
        data_check_lambda = lambda_python.PythonFunction(
            self,
            f"{pipeline_name}DataFreshnessCheck",
            function_name=f"{pipeline_name}-DataFreshnessCheck",
            description=f"Check conditions of data freshness for {pipeline_name}",
            entry="lambdas/functions/datafreshness-check",
            index="lambda_function.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=cdk.Duration.seconds(120),
            environment={"TOPIC_ARN": topic.topic_arn},
            role=lambda_role
        )
        topic.grant_publish(data_check_lambda)
        project_bucket.grant_read_write(data_check_lambda)

        data_check_lambda.grant_invoke(sagemaker_execution_role)
        callback_queue.grant_send_messages(sagemaker_execution_role)

        pipeline_conf = pipeline_props["pipeline_configuration"]
        for k, o in pipeline_conf.items():
            if "_fg_name" in k:
                pipeline_conf[k] = f"{project_name}-{o}"
        pipeline_conf["datafreshness_func_arn"] = data_check_lambda.function_arn
        pipeline_conf["queue_url"] = callback_queue.queue_url
        pipeline_conf["model_package_group_name"] = model_package_group_name
        pipeline_conf["features_names"] = features_names

        try:
            logging.info("Attempting to generate pipeline definition")
            pipeline_definition = generate_pipeline_definition(
                role=sagemaker_execution_role.role_arn,
                region=os.getenv("AWS_REGION"),
                default_bucket=project_bucket_name,
                pipeline_name=pipeline_name,
                pipeline_conf=pipeline_conf,
                code_file_path=pipeline_props["code_file_path"],
            )

            sagemaker.CfnPipeline(
                self,
                f"SageMakerPipeline-{pipeline_name}",
                pipeline_name=pipeline_name,
                pipeline_definition={"PipelineDefinitionBody": pipeline_definition},
                role_arn=sagemaker_execution_role.role_arn,
                tags=tags
            )
            logging.info("SageMaker Pipeline defined")

            inference_lambda = GlueDynamoDb(
                self,
                f"UploadResults-{pipeline_name}",
                callback_queue=callback_queue,
                model_name=pipeline_name,
                index_name=pipeline_props["index_name"]
            ).function_read_ddb
            get_data_ddb_integration = apigateway.LambdaIntegration(inference_lambda)

            get_data_ddb = api_gw.root.add_resource(f"get-{pipeline_name}")
            get_data_ddb.add_method(
                http_method="GET", integration=get_data_ddb_integration
            )
            endpoint_parameter = ssm.StringParameter(
                self,
                f"{pipeline_name}-URL",
                string_value=api_gw.url_for_path(path=get_data_ddb.path),
                parameter_name=f"/sagemaker-{project_name}/{pipeline_name}",
            )

        except:
            logging.exception("Failed to create a Pipeline definition")
