import py4j
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W
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
from pyspark.sql.functions import udf, col, size, collect_list, collect_list,lower,when

@udf()
def generate_random_string():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=27))



def compute_ucp(config_path, profile_path, dedupe_path, ucp_path, spark, partition_date, LOGGER):
    LOGGER.info("ccai -> ucp started")
    df_profile = spark.read.parquet(profile_path)
    df_dedupe = spark.read.parquet(dedupe_path)
    df_profile.createOrReplaceTempView("profile")
    df_dedupe.createOrReplaceTempView("dedupe")
    num_rows_in = df_profile.count()
    num_rows_dedupe = df_dedupe.count()
    df_profile = spark.sql("select a.*,b.passenger_hash from profile as a left join dedupe as b on a.ProvisionalPrimaryKey = b.ProvisionalPrimaryKey")
    df_profile = df_profile.withColumn("passenger_hash", F.max("passenger_hash").over(W().partitionBy("firstname","lastname","dateofbirth")))
    df_profile = df_profile.withColumn("passenger_hash", F.when(F.col("passenger_hash").isNull(), generate_random_string()).otherwise(F.col("passenger_hash")))

    ##change1
    df_profile = df_profile.withColumn("mfirstname", when(col("firstname").isNotNull(), lower(col("firstname"))).otherwise("Unknown"))
    df_profile = df_profile.withColumn("mlastname", when(col("lastname").isNotNull(), lower(col("lastname"))).otherwise("Unknown"))

    df_profile = df_profile.withColumn("passenger_hash", F.max("passenger_hash").over(W().partitionBy("mfirstname","mlastname","dateofbirth")))

    ##change 2##
    df_profile = df_profile.drop("mfirstname", "mlastname")

    df_profile.write.parquet(ucp_path, mode="overwrite")

    df_profile_out = spark.read.parquet(ucp_path)
    LOGGER.info("ccai -> Num Rows Dedupe : " + str(num_rows_dedupe))
    LOGGER.info("ccai -> Num Rows Input : " + str(num_rows_in))
    LOGGER.info("ccai -> Num Rows Output : " + str(df_profile_out.count()))
    LOGGER.info("ccai -> Num fn, ln, dob Combinations : " + str(df_profile_out.select("firstname","lastname","dateofbirth").dropDuplicates().count()))
    LOGGER.info("ccai -> Num Passenger Hash : " + str( df_profile_out.select("passenger_hash").dropDuplicates().count()))
    LOGGER.info("ccai -> Num Rows Without Hash : " + str(df_profile_out.filter("passenger_hash is null").count()))
    ambiguous_hash = df_profile_out.groupBy("firstname","lastname","dateofbirth").agg(F.countDistinct("passenger_hash").alias("numHash")).filter("numHash>1")
    ambiguous_hash.cache()
    LOGGER.info("ccai -> Num Ambiguous Hashes : " + str(ambiguous_hash.count()))
    LOGGER.info("ccai -> Ambiguous Hash Samples : \n" + str( ambiguous_hash.orderBy(F.desc("numHash")).limit(10).toPandas().to_csv(index=False)))
    LOGGER.info("ccai -> ucp done")
    return ucp_path