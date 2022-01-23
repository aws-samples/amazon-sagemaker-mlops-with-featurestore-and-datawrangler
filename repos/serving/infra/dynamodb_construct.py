import logging
import os

import aws_cdk as cdk
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as lambda_event_sources
from aws_cdk import aws_lambda_python_alpha as lambda_python
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks
from constructs import Construct

from infra.serving_stack_utils import upload_file_to_bucket

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

        project_bucket = s3.Bucket.from_bucket_name(
            self, "ProjectBucket", bucket_name=project_bucket_name
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
        logger.info("Upload Glue Job script ...")
        source_file_full_path = "./scripts/glue/load-ddb-table.py"
        target_key_name_glue_job = "glue/scripts/load-ddb-table.py"
        upload_file_to_bucket(
            source_file_full_path, project_bucket_name, target_key_name_glue_job
        )

        logger.info("Create Glue Job and attach the pre-created role")
        glue_job = glue.CfnJob(
            scope=self,
            id=f"sagemaker-{project_id}-GlueJob",
            name=f"sagemaker-{project_id}-GlueJob",
            description="Glue Job to upload the result of Batch Transform to DynamoDB for low-latency serving",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{project_bucket_name}/glue/scripts/load-ddb-table.py",
            ),
            default_arguments={
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-metrics": "",
                "--additional-python-modules": "pyarrow==2,awswrangler==2.9.0",
                "--TARGET_DDB_TABLE": table_ddb.table_name,
                "--SOURCE_S3_BUCKET": project_bucket_name,
                "--TABLE_HEADER_NAME": f"{index_name}, score",
            },
            glue_version="3.0",
            worker_type="Standard",
            number_of_workers=2,
            timeout=15,
            max_retries=0,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=3
            ),
        )

        #### Lambda Functions for the Step Function
        logger.info(
            "Create AWS Lambda Python Function (Call Glue Job) > to be connected to a Step Function"
        )

        lambda_role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["glue:StartJobRun"],
                resources=[f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:job/sagemaker-*"],
            )
        )

        lambda_role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["glue:GetJobRun", "glue:GetJobRuns", "glue:GetJobs"],
                resources=["*"],
            )
        )


        function_sfn_job_exec = lambda_python.PythonFunction(
            self,
            "SFNJobExec",
            function_name=f"sagemaker-{project_id}-SFNJobExec",
            entry="lambdas/functions/processing-job-execution",
            index="lambda_function.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=cdk.Duration.minutes(15),
            environment={},
            role=lambda_role,
        )

        logger.info(
            "Create AWS Lambda Python Function (Check Glue Job Status) > to be connected to a Step Function"
        )
        function_sfn_job_status_check = lambda_python.PythonFunction(
            self,
            "SFNJobStatusCheck",
            function_name=f"sagemaker-{project_id}-SFNJobStatusCheck",
            entry="lambdas/functions/processing-job-status-check",
            index="lambda_function.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=cdk.Duration.minutes(15),
            environment={},
            role=lambda_role,
        )

        ##################################################################################################################################################################
        #########################                                                      STEP FUNCTIONS                                         ############################
        ##################################################################################################################################################################
        # Based on https://docs.aws.amazon.com/step-functions/latest/dg/sample-project-job-poller.html
        #       https://docs.aws.amazon.com/cdk/api/latest/python/aws_cdk.aws_stepfunctions/README.html#example

        logger.info("Step Function State Machine Activities ...")
        logger.info("1\ Create Step Function tasks (steps) ")
        logger.info("2\ Register steps to a Step Function definition")
        logger.info("3\ Create a Step Function state machine using definition")

        submit_job = sfn_tasks.LambdaInvoke(
            self,
            "Submit Job",
            lambda_function=function_sfn_job_exec,
            result_path="$.body.job",
        )

        wait_x = sfn.Wait(
            self, "Wait", time=sfn.WaitTime.duration(cdk.Duration.seconds(15))
        )

        get_status = sfn_tasks.LambdaInvoke(
            self,
            "Get Job Status",
            lambda_function=function_sfn_job_status_check,
            result_path="$.body.job",
        )

        job_failed = sfn.Fail(
            self,
            "Job Failed",
            cause="AWS Job Failed",
            error="DescribeJob returned FAILED",
        )

        final_status = sfn_tasks.LambdaInvoke(
            self,
            "Get Final Job Status",
            lambda_function=function_sfn_job_status_check,
        )

        definition = (
            submit_job.next(wait_x)
            .next(get_status)
            .next(
                sfn.Choice(self, "Job Complete?")
                .when(
                    sfn.Condition.string_equals(
                        "$.body.job.Payload.jobDetails.jobStatus", "FAILED"
                    ),
                    job_failed,
                )
                .when(
                    sfn.Condition.string_equals(
                        "$.body.job.Payload.jobDetails.jobStatus", "SUCCEEDED"
                    ),
                    final_status,
                )
                .otherwise(wait_x)
            )
        )

        statemachine = sfn.StateMachine(
            self,
            "StateMachineMLOps",
            state_machine_name=f"sagemaker-{project_id}-DynamoDB_Loader",
            definition=definition,
            timeout=cdk.Duration.minutes(15),
            role=iam.Role.from_role_arn(
                self, "LambdaRoleImmutable", role_arn=lambda_role_arn, mutable=False
            ),
        )

        logger.info(
            "Create AWS Lambda Python Function (trigger step function) > to be connected to an SQS queue"
        )

        function_execute_sfn = lambda_python.PythonFunction(
            self,
            "SFNExecute",
            function_name=f"sagemaker-{project_id}-SFNExecute",
            entry="lambdas/functions/execute-state-machine",
            index="lambda_function.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=cdk.Duration.seconds(100),
            environment={
                "state_machine_arn": statemachine.state_machine_arn,
                "TARGET_GLUE_JOB": glue_job.name,
                "TARGET_DDB_TABLE": table_ddb.table_name,
            },
            role=lambda_role,
        )

        function_execute_sfn.add_event_source(
            lambda_event_sources.SqsEventSource(callback_queue)
        )
