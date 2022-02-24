
"""
This Lambda parses the output of ModelQualityStep to extract the value of a specific metric
"""

import json
import boto3

sm_client = boto3.client("sagemaker")
s3 = boto3.resource('s3')

def lambda_handler(event, context):
    # model quality report URI
    model_quality_report_uri = event['model_quality_report_uri']
    metric_name = event['metric_name']
    
    
    o = s3.Object(*split_s3_path(model_quality_report_uri))
    retval = json.load(o.get()['Body'])
    
    metrics = json.load(o.get()['Body'])

    return {
        "statusCode": 200,
        "body": json.dumps(f"Extracted {metric_name}"),
        "metric_value": metrics['binary_classification_metrics'][metric_name]['value']
    }

def split_s3_path(s3_path):
    path_parts=s3_path.replace("s3://","").split("/")
    bucket=path_parts.pop(0)
    key="/".join(path_parts)
    return bucket, key
