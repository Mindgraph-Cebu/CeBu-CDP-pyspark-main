import py4j
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType, DateType, TimestampType, MapType, ArrayType
import boto3
import json
# import SparkConf
from pyspark import SparkConf
import pyspark.pandas as ps
import random
import numpy as np
import datetime
import random
import string
from pyspark.sql.functions import udf, col, size, collect_list, collect_list

@udf()
def generate_random_string():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=27))

def compute_ucp(config_path, profile_path, dedupe_path, ucp_path, spark, partition_date, LOGGER):
    LOGGER.info("ccai ucp started")
    df_profile = spark.read.parquet(profile_path)
    df_dedupe = spark.read.parquet(dedupe_path)
    df_profile.createOrReplaceTempView("profile")
    df_dedupe.createOrReplaceTempView("dedupe")
    df_profile = spark.sql("select a.*,b.passenger_hash from profile as a left join dedupe as b on a.ProvisionalPrimaryKey = b.ProvisionalPrimaryKey")
    df_profile = df_profile.withColumn("passenger_hash", F.when(F.col("passenger_hash").isNull(), generate_random_string()).otherwise(F.col("passenger_hash")))
    df_profile.write.parquet(ucp_path, mode="overwrite")
    LOGGER.info("ccai ucp done")
    return ucp_path
