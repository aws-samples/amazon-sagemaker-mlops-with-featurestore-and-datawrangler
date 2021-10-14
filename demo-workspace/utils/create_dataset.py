import argparse
from pathlib import Path

import pandas as pd

training_columns = [
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

# Parse argument variables passed via the CreateDataset processing step
parser = argparse.ArgumentParser()
parser.add_argument("--athena-data", type=str)
args = parser.parse_args()

dataset_path = Path("/opt/ml/processing/output/dataset")
dataset = pd.read_parquet(args.athena_data, engine="pyarrow")

# Write train, test splits to output path
dataset_output_with_index_path = Path("/opt/ml/processing/output_with_index/dataset")
dataset.to_csv(dataset_output_with_index_path / "dataset.csv", index=False, header=True)

dataset = dataset[training_columns]

# Write train, test splits to output path
dataset_output_path = Path("/opt/ml/processing/output/dataset")
dataset.to_csv(dataset_output_path / "dataset.csv", index=False, header=False)
