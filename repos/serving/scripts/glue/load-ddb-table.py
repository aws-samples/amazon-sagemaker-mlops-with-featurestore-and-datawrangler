import logging
import sys
import time

import awswrangler as wr
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "TARGET_DDB_TABLE",
        "S3_BUCKET",
        "S3_PREFIX_PROCESSED",
        "TABLE_HEADER_NAME",
    ],
)

target_ddb_table = args["TARGET_DDB_TABLE"]

s3_bucket = args["S3_BUCKET"]
s3_prefix_processed = args["S3_PREFIX_PROCESSED"]
table_header_name = args["TABLE_HEADER_NAME"].split(",")

logger.info("Read processed file (model pipeline output) (no header) ...")
source_s3_proc = f"s3://{s3_bucket}/{s3_prefix_processed}"
input_df_proc = wr.s3.read_csv(source_s3_proc, header=None, chunksize=1000)

glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info("Target DDB Table: [{}]".format(target_ddb_table))
logger.info("START: Loading data to DDB Table ...")

t2 = time.time()

rec_cnt = 0
# Read CSV file(s) in chunks/sets of 1000 records and load to DDB table
for input_df in input_df_proc:
    # convert all columns to str
    input_df = input_df.iloc[:, [0, -1]]
    input_df.columns = table_header_name
    input_df = input_df.astype(str)
    rec_cnt += input_df.shape[0]
    wr.dynamodb.put_df(df=input_df, table_name=target_ddb_table)
    time.sleep(1)

output2 = time.time() - t2

logger.info("END  : Loading data to DDB Table ...")
logger.info("Loading time: [{}] seconds".format(output2))
logger.info("No. of records loaded: [{}]".format(rec_cnt))

job.commit()
