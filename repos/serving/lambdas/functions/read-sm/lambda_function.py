import os
import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from decimal import Decimal
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

# Retrieve region where Lambda is being executed
region_name = os.environ['AWS_REGION'] #"ap-southeast-1"

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# Create DynamoDB resource
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):

    logger.info('Lambda event is [{}]'.format(event))

    # Retrieve AWS request identifier for this execution
    request_id = str(context.aws_request_id)

    # Retrieve target DynamoDB table name from environment variables
    val_table_name = os.environ['target_ddb_table'] # mlops-table

    val_policy_id = str(event['queryStringParameters']['policy_id'])

    logger.info('Input parameters are [{}] [{}]'.format(val_table_name, val_policy_id))

    request_data = event.get("requestContext")

    logger.info('Request data is is [{}]'.format(request_data))

    # Create a paginator
    paginator = dynamodb.meta.client.get_paginator('query')

    logger.info('Retrieving data ...')

    try:

        # Scenario : retrieve all columns from the source table
        response_iterator = paginator.paginate(
            TableName=val_table_name,
            KeyConditionExpression=Key('policy_id').eq(val_policy_id),
            #FilterExpression=Attr('cnt_records').gte(val_record_cnt),
            Select='ALL_ATTRIBUTES',
            PaginationConfig={'MaxItems':10, 'PageSize': 10}
        )

        for page in response_iterator:
            #logger.info('count->[{}]'.format(page['Count']))
            if page['Count'] > 0:
                converted_items = json.dumps(page['Items'], cls=DecimalEncoder, indent = 2)

        logger.info('no. of items ->[{}]'.format(len(converted_items)))

        # Dummy response
        responsebody = converted_items

        # Generate response
        responseObject = {}
        responseObject['statusCode'] = 200
        responseObject['headers'] = {}
        responseObject['headers']['Content-Type'] = 'application/json'
        responseObject['body'] = responsebody

        return(responseObject)
    except ClientError as e:
        if e.response['Error']['Code'] == "AccessDeniedException":
            logger.error('Error - AccessDeniedException', exc_info=True)

            return {
            'statusCode': 401,
            'body': json.dumps('Insufficient rights to perform this operation')
            }
        else:
            raise