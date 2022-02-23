import json
import re
from pathlib import Path
from typing import List, Union

from aws_cdk import CfnTag
from aws_cdk import aws_sagemaker as sagemaker
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
    "Fractional": FeatureTypeEnum.FRACTIONAL,
    "Integral": FeatureTypeEnum.INTEGRAL,
}


def prepare_features_definitions(
    column_schemas: dict,
) -> List[sagemaker.CfnFeatureGroup.FeatureDefinitionProperty]:

    feature_definitions = [
        FeatureDefinition(
            feature_name=column_schema["FeatureName"],
            feature_type=column_to_feature_type_mapping.get(
                column_schema["FeatureType"], default_feature_type
            ),
        ).to_dict()
        for column_schema in column_schemas
    ]

    return [
        sagemaker.CfnFeatureGroup.FeatureDefinitionProperty(
            **{key_map[k]: o for k, o in j.items()}
        )
        for j in feature_definitions
    ]


def get_fg_conf(file_path: Union[str, Path], bucket_name: str = None) -> dict:
    with open(file_path, "r") as f:
        f_list = json.load(f)
    f_list = {pascal2snake(k): v for k, v in f_list.items()}
    f_list["feature_definitions"] = prepare_features_definitions(
        f_list["feature_definitions"]
    )
    if f_list["offline_store_config"]:
        f_list["offline_store_config"] = dict(
            DisableGlueTableCreation=False,
            S3StorageConfig={"S3Uri": f"s3://{bucket_name}/"},
        )
    try:
        f_list["tags"] = [
            CfnTag(**{pascal2snake(j): o for j, o in k.keys()})
            for k in f_list["tags"]
            if (k["Key"].lower() == "stage") & (k["Value"].lower() != "dev")
        ]
    except:
        logger.exception("Something went wrong")
        pass
    return f_list


def pascal2snake(name):
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def snake2pascal(test_str: str):
    return test_str.replace("_", " ").title().replace(" ", "")
