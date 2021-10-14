import json
import logging
import os

import boto3
import pandas as pd

logger = logging.getLogger()

region = os.environ["region"]
endpoint_name = os.environ["endpoint_name"]
content_type = os.environ["content_type"]
customers_fg_name = os.environ["customers_fg_name"]
claims_fg_name = os.environ["claims_fg_name"]

boto_session = boto3.Session(region_name=region)
featurestore_runtime = boto_session.client(
    service_name="sagemaker-featurestore-runtime", region_name=region
)
client_sm = boto_session.client("sagemaker-runtime", region_name=region)

col_order = [
    "incident_severity",
    "num_vehicles_involved",
    "num_injuries",
    "num_witnesses",
    "police_report_available",
    "injury_claim",
    "vehicle_claim",
    "total_claim_amount",
    "incident_month",
    "incident_day",
    "incident_dow",
    "incident_hour",
    "driver_relationship_self",
    "driver_relationship_na",
    "driver_relationship_spouse",
    "driver_relationship_child",
    "driver_relationship_other",
    "incident_type_collision",
    "incident_type_breakin",
    "incident_type_theft",
    "collision_type_front",
    "collision_type_rear",
    "collision_type_side",
    "collision_type_na",
    "authorities_contacted_police",
    "authorities_contacted_none",
    "authorities_contacted_fire",
    "authorities_contacted_ambulance",
    "customer_age",
    "customer_education",
    "months_as_customer",
    "policy_deductable",
    "policy_annual_premium",
    "policy_liability",
    "auto_year",
    "num_claims_past_year",
    "num_insurers_past_5_years",
    "customer_gender_male",
    "customer_gender_female",
    "policy_state_ca",
    "policy_state_wa",
    "policy_state_az",
    "policy_state_or",
    "policy_state_nv",
    "policy_state_id",
]


def lambda_handler(event, context):
    # Get data from online feature store
    logger.info(event)
    val_policy_id = str(event["queryStringParameters"]["policy_id"])

    claims_response = featurestore_runtime.get_record(
        FeatureGroupName=claims_fg_name,
        RecordIdentifierValueAsString=str(val_policy_id),
    )

    if claims_response.get("Record"):
        claims_record = claims_response["Record"]
        claims_df = pd.DataFrame(claims_record).set_index("FeatureName")
    else:
        logging.info("No Record returned / Record Key in claims feature group\n")
        return {
            "statusCode": 404,
            "body": json.dumps({"Error": "Record not found in CLAIMS feature group"}),
        }

    customers_response = featurestore_runtime.get_record(
        FeatureGroupName=customers_fg_name,
        RecordIdentifierValueAsString=str(val_policy_id),
    )

    if customers_response.get("Record"):
        customer_record = customers_response["Record"]
        customer_df = pd.DataFrame(customer_record).set_index("FeatureName")
    else:
        logging.info("No Record returned / Record Key in CUSTOMERS feature group\n")
        return {
            "statusCode": 404,
            "body": json.dumps(
                {"Error": "Record not found in CUSTOMERS feature group"}
            ),
        }

    try:
        blended_df = pd.concat([claims_df, customer_df]).loc[col_order]
        data_input = ",".join(blended_df["ValueAsString"])

        logging.info("data_input: ", data_input)
        response = client_sm.invoke_endpoint(
            EndpointName=endpoint_name, Body=data_input, ContentType=content_type
        )

        score = json.loads(response["Body"].read())
        logging.info(f"score: {score}")

        return {
            "statusCode": 200,
            "body": json.dumps({"policy_id": val_policy_id, "score": score}),
        }
    except Exception:
        logging.exception(f"internal error")
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"Error": f"internal error. Check Logs for more details"}
            ),
        }
