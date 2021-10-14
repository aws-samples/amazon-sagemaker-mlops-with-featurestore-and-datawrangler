import json
from pathlib import Path
from typing import List, Union

from sagemaker.feature_store.feature_definition import (
    FeatureDefinition,
    FeatureTypeEnum,
)

key_map = dict(
    FeatureName="feature_name",
    FeatureType="feature_type",
    name="feature_name",
    type="feature_type",
)

default_feature_type = FeatureTypeEnum.STRING
column_to_feature_type_mapping = {
    "float": FeatureTypeEnum.FRACTIONAL,
    "long": FeatureTypeEnum.INTEGRAL,
}


def prepare_features_definitions(
    column_schemas: dict,
):

    feature_definitions = [
        FeatureDefinition(
            feature_name=column_schema["name"],
            feature_type=column_to_feature_type_mapping.get(
                column_schema["type"], default_feature_type
            ),
        )
        for column_schema in column_schemas
    ]

#     return [
#             {key_map[k]: o for k, o in j.items()}
#         for j in feature_definitions
#     ]
    return feature_definitions


def get_fg_conf(file_path: Union[str, Path], s3_uri: str = None) -> dict:
    with open(file_path, "r") as f:
        f_list = json.load(f)
#     offline_conf = None
#     if f_list["offline_store_config"]:
#         offline_conf = dict(
#             DisableGlueTableCreation=False,
#             S3StorageConfig={"S3Uri": f"s3://{bucket_name}/"},
#         )

    return dict(
        feature_group_name=f_list["feature_group_name"],
        feature_group_properties=dict(
            s3_uri=s3_uri,
            event_time_feature_name=f_list["event_time_feature_name"],
            record_identifier_name=f_list["record_identifier_feature_name"],
            enable_online_store=f_list["enable_online_store"],
            disable_glue_table_creation=f_list["disable_glue_table_creation"],
        ),
        feature_definitions=prepare_features_definitions(f_list["column_schemas"]),
    )
