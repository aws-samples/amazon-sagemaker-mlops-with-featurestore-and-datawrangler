import logging
import os

import aws_cdk as cdk
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_glue_alpha as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as lambda_event_sources
from aws_cdk import aws_lambda_python_alpha as lambda_python
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks
from constructs import Construct

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

project_bucket_name = os.getenv("PROJECT_BUCKET")
project_name = os.getenv("SAGEMAKER_PROJECT_NAME")
project_id = os.getenv("SAGEMAKER_PROJECT_ID")
codepipeline_arn = os.getenv("CODEPIPELINE_ARN")
execution_role_arn = os.getenv("SAGEMAKER_PIPELINE_ROLE_ARN")
glue_role_arn = os.getenv("GLUE_ROLE_ARN")
api_gateway_role_arn = os.getenv("API_GATEWAY_ROLE_ARN")
lambda_role_arn = os.getenv("LAMBDA_ROLE_ARN")


class GlueDynamoDb(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        callback_queue: sqs.Queue,
        model_name: str,
        index_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        glue_role = iam.Role.from_role_arn(self, "GlueRole", role_arn=glue_role_arn)
        lambda_role = iam.Role.from_role_arn(
            self, "LambdaRole", role_arn=lambda_role_arn
        )

        logger.info("Create DynamoDB Table")
        table_ddb = dynamodb.Table(
            self,
            "DDBTable",
            table_name=f"sagemaker-{project_id}-{model_name}-DDB-Table",
            partition_key=dynamodb.Attribute(
                name="policy_id", type=dynamodb.AttributeType.STRING
            ),
            removal_policy=cdk.RemovalPolicy.DESTROY,
            read_capacity=5,
            write_capacity=100,
        )

        # IAM Role (Glue)
        logger.info("Update IAM Role (Glue Job)")
        glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            )
        )

        logger.info("Add grant to Glue job role")
        table_ddb.grant_read_write_data(glue_role)

        logger.info(
            "Create AWS Lambda Python Function (Read DynamoDB Table) > to be connected to API GW"
        )
        self.function_read_ddb = lambda_python.PythonFunction(
            self,
            "ReadDDBTable",
            function_name=f"sagemaker-{project_id}-ReadDDBTable",
            entry="lambdas/functions/read-ddb",
            index="lambda_function.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=cdk.Duration.seconds(15),
            environment={"target_ddb_table": table_ddb.table_name},
            role=lambda_role,
        )

        table_ddb.grant_read_data(self.function_read_ddb)

        # Glue
        logger.info("Create Glue Job and attach the pre-created role")
        glue_job = glue.Job(
            self,
            f"sagemaker-{project_id}-GlueJob",
            job_name=f"sagemaker-{project_id}-GlueJob",
            executable=glue.JobExecutable.python_etl(
                glue_version=glue.GlueVersion.V3_0,
                python_version=glue.PythonVersion.THREE,
                script=glue.Code.from_asset(path="./scripts/glue/load-ddb-table.py"),
            ),
            role=glue_role,
            description="Glue Job to upload the result of Batch Transform to DynamoDB for low-latency serving",
            default_arguments={
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-metrics": "",
                "--additional-python-modules": "pyarrow==2,awswrangler==2.9.0",
                "--TARGET_DDB_TABLE": table_ddb.table_name,
                "--SOURCE_S3_BUCKET": project_bucket_name,
                "--TABLE_HEADER_NAME": f"{index_name}, score",
            },
            worker_count=2,
            worker_type=glue.WorkerType.STANDARD,
            max_concurrent_runs=3,
            timeout=cdk.Duration.minutes(15),
        )

        # STEP FUNCTION
        start_glue_job = sfn_tasks.GlueStartJobRun(
            self,
            "StartGlueJobTask",
            glue_job_name=glue_job.job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.taskresult",
            arguments=sfn.TaskInput.from_object(
                {
                    "--job-bookmark-option": "job-bookmark-enable",
                    "--additional-python-modules": "pyarrow==2,awswrangler==2.9.0",
                    "--TARGET_DDB_TABLE": table_ddb.table_name,
                    "--S3_BUCKET": sfn.JsonPath.string_at("$.body.bucket"),
                    "--S3_PREFIX_PROCESSED": sfn.JsonPath.string_at(
                        "$.body.keysRawProc[0]"
                    ),
                }
            ),
        )

        send_success = sfn_tasks.CallAwsService(
            self,
            "SendSuccess",
            iam_resources=["sagemaker:SendPipelineExecutionStepSuccess"],
            service="sagemaker",
            action="sendPipelineExecutionStepSuccess",
            parameters={"CallbackToken.$": "$.callbackToken"},
        )
        send_failure = sfn_tasks.CallAwsService(
            self,
            "SendFailure",
            iam_resources=["sagemaker:SendPipelineExecutionStepFailure"],
            service="sagemaker",
            action="sendPipelineExecutionStepFailure",
            parameters={"CallbackToken.$": "$.callbackToken"},
        )

        definition = start_glue_job.add_catch(
            send_failure,
            result_path="$.error-info",
        ).next(
            sfn.Choice(self, "Job successful?")
            .when(
                sfn.Condition.string_equals("$.taskresult.JobRunState", "SUCCEEDED"),
                send_success,
            )
            .otherwise(
                send_failure,
            )
        )

        statemachine = sfn.StateMachine(
            self,
            "StateMachineMLOps",
            state_machine_name=f"sagemaker-{project_id}-DynamoDB_Loader",
            definition=definition,
            timeout=cdk.Duration.minutes(15),
            role=iam.Role.from_role_arn(
                self,
                "LambdaRoleImmutable",
                role_arn=lambda_role_arn,
                mutable=False,
            ),
        )

        function_execute_sfn = lambda_python.PythonFunction(
            self,
            "SFNExecute",
            function_name=f"sagemaker-{project_id}-SFNExecute",
            entry="lambdas/functions/execute-state-machine",
            index="lambda_function.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=cdk.Duration.seconds(10),
            environment={
                "state_machine_arn": statemachine.state_machine_arn,
                "TARGET_DDB_TABLE": table_ddb.table_name,
            },
            role=lambda_role,
        )

        function_execute_sfn.add_event_source(
            lambda_event_sources.SqsEventSource(callback_queue)
        )
