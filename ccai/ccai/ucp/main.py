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
import json
import py4j
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import ceja
import jellyfish as J

# import recordlinkage
import pandas as pd
import re
import hashlib
import textdistance
from abydos.phonetic import SpanishMetaphone
from unidecode import unidecode
import networkx
from networkx.algorithms.components.connected import connected_components
from pyspark import SparkContext, SparkConf
import ibis
import random
import string
from pyspark.sql.functions import udf
from datetime import datetime as dt

@udf()
def generate_random_string():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=27))

@udf(returnType=T.BooleanType())
def get_date_similarity(d1, d2):
    def getCompScore(s1, s2):
        if (s1.strip() == s2.strip()):
            return 1
        elif (J.damerau_levenshtein_distance(s1.strip(), s2.strip()) <= 1):
            return 0.5
        return 0
    if (d1 is None) or (d2 is None):
        return False
    if (d1.strip()==d2.strip()):
        return True
    try:
        y1,m1,day1 = d1.strip().split("-")
        y2,m2,day2 = d2.strip().split("-")
        score=getCompScore(y1,y2)+getCompScore(m1,m2)+getCompScore(d1,d2)
        return score>=2.5
    except:
        return False

@udf(returnType=T.StringType())
def unidecode_encode(name):
    try:
        return unidecode(name)
    except:
        return "xxx"

def phonetic_encode(sm, name):
    try:
        if len(str(name)) > 3:
            return sm.encode(name)
        else:
            return str(name)
    except:
        return "CCAI_NULL"

def get_old_passengers_paths(new_passengers_base_path,day0_date,end_date):
    bucket_name = new_passengers_base_path.split("/")[2]
    prefix = "new_passengers/"
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

    paths = []
    for common_prefix in response.get('CommonPrefixes', []):
        folder_name = common_prefix['Prefix']
        date_str = folder_name.split("=")[-1].rstrip('/')
        if day0_date <= date_str <= end_date:
            s3_path = 's3a://{}/new_passengers/p_date={}/'.format(bucket_name,date_str)
            paths.append(s3_path)
    return paths


def get_config(file_path):
    # file_path = "s3a://cebu-cdp-data-qa/script/glue/cebu-cdp-profile-glue/profile_config_cebu_v1.json"
    bucket_name = file_path.split('/')[2]
    key = '/'.join(file_path.split('/')[3:])
    s3 = boto3.client('s3')
    config = json.loads(s3.get_object(
        Bucket=bucket_name, Key=key)['Body'].read())
    return config


def compute_ucp(config_path, profile_path, dedupe_path, ucp_path, spark,day0_date, end_date, LOGGER, incremental):
    config = get_config(config_path)
    partition_date = end_date
    new_passengers_base_path = config["storageDetails"][5]["pathUrl"]
    LOGGER.info("ccai -> ucp started")
    df_profile = spark.read.parquet(profile_path)
    df_dedupe = spark.read.parquet(dedupe_path)
    df_profile.createOrReplaceTempView("profile")
    df_dedupe.createOrReplaceTempView("dedupe")
    num_rows_in = df_profile.count()
    num_rows_dedupe = df_dedupe.count()

    df_profile = spark.sql("select a.*,b.passenger_hash from profile as a left join dedupe as b on a.ProvisionalPrimaryKey = b.ProvisionalPrimaryKey")
    df_profile = df_profile.withColumn("passenger_hash", F.max("passenger_hash").over(W().partitionBy("FirstName","LastName","DateOfBirth")))
    df_profile = df_profile.withColumn("passenger_hash", F.when(F.col("passenger_hash").isNull(), generate_random_string()).otherwise(F.col("passenger_hash")))

    ##change1
    df_profile = df_profile.withColumn("mfirstname", when(col("FirstName").isNotNull(), lower(col("FirstName"))).otherwise("Unknown"))
    df_profile = df_profile.withColumn("mlastname", when(col("LastName").isNotNull(), lower(col("LastName"))).otherwise("Unknown"))

    df_profile = df_profile.withColumn("passenger_hash", F.max("passenger_hash").over(W().partitionBy("mfirstname","mlastname","DateOfBirth")))

    ##change 2##
    df_profile = df_profile.drop("mfirstname", "mlastname")

    firstname_filters = [
        "tba",
        "adt1",
        "adt2",
        "pax",
        "fname",
        "first",
        "xxx",
        "firstname",
        "api",
        "anonymous",
        "xxx",
        "cebitcc",
        "ntba",
        "test",
        "itcc",
        "sherwin",
        "unknown",
        "ccainull"
    ]
    lastname_filters = [
        "tbaa",
        "last",
        "aaa",
        "adt1",
        "adt2",
        "bcs",
        "pax",
        "tba",
        "lname",
        "xxx",
        "andrada",
        "commandcenter",
        "ceb",
        "lastname",
        "unknown",
        "ccainull"
    ]

    sm = SpanishMetaphone()
    phonetic_encode_udf = F.udf(lambda x: phonetic_encode(sm, x), T.StringType())

    if incremental == "False" or incremental == False:

        df_profile.write.parquet(ucp_path, mode="overwrite")

        df_profile_out = spark.read.parquet(ucp_path)
        df_profile_out.cache()
        LOGGER.info("ccai -> Num Rows Dedupe : " + str(num_rows_dedupe))
        LOGGER.info("ccai -> Num Rows Input : " + str(num_rows_in))
        LOGGER.info("ccai -> Num Rows Output : " + str(df_profile_out.count()))
        LOGGER.info("ccai -> Num fn, ln, dob Combinations : " + str(df_profile_out.select("FirstName","LastName","DateOfBirth").dropDuplicates().count()))
        LOGGER.info("ccai -> Num Passenger Hash : " + str( df_profile_out.select("passenger_hash").dropDuplicates().count()))
        LOGGER.info("ccai -> Num Rows Without Hash : " + str(df_profile_out.filter("passenger_hash is null").count()))
        ambiguous_hash = df_profile_out.groupBy("FirstName","LastName","DateOfBirth").agg(F.countDistinct("passenger_hash").alias("numHash")).filter("numHash>1")
        ambiguous_hash.cache()
        LOGGER.info("ccai -> Num Ambiguous Hashes : " + str(ambiguous_hash.count()))
        LOGGER.info("ccai -> Ambiguous Hash Samples : \n" + str( ambiguous_hash.orderBy(F.desc("numHash")).limit(10).toPandas().to_csv(index=False)))
        LOGGER.info("ccai -> ucp done")



        new_passengers = df_profile_out.filter("(FirstName is not null) and (LastName is not null) and (DateOfBirth is not null) and (DateOfBirth>='1947') and (DateOfBirth not like '%xxx%')")\
                                       .select(F.lower(F.regexp_replace(unidecode_encode("FirstName"), "[^a-zA-Z0-9 ]", "")).alias("FirstName"),
                                               F.lower(F.regexp_replace(unidecode_encode("LastName"), "[^a-zA-Z0-9 ]", "")).alias("LastName"),
                                               F.col("DateOfBirth").alias("DateOfBirth"),
                                               F.col("passenger_hash").alias("passenger_hash"))\
                                       .filter("(FirstName not in ('{}')) and (LastName not in ('{}'))".format("','".join(firstname_filters), "','".join(lastname_filters)))\
                                       .withColumn("pFirstName",F.trim(F.regexp_replace(F.regexp_replace(F.col("FirstName"), "\.", " "), "^(mr|ms|dr|rev|prof|sir|madam|miss|mrs|st)", "")))\
                                       .withColumn("pFirstName", F.when(F.col("DateOfBirth").like("9999%"),F.lower("pFirstName")).otherwise(F.trim(phonetic_encode_udf(F.split(F.lower("pFirstName"), " ").getItem(0))))) \
                                       .withColumn("pLastName", F.when(F.col("DateOfBirth").like("9999%"), F.lower("LastName")).otherwise(F.trim(F.lower(phonetic_encode_udf(F.col("LastName")))))) \
                                       .select("pFirstName","pLastName","DateOfBirth","passenger_hash").dropDuplicates()
        
        #.withColumn("pFirstName", F.when(F.col("DateOfBirth").like("9999%"), F.lower("pFirstName")).otherwise(F.trim(F.split(phonetic_encode_udf(F.lower("pFirstName")), " ").getItem(0))))


        new_passengers.write.parquet(new_passengers_base_path+"/p_date={}".format(partition_date), mode='overwrite')

    else:
        LOGGER.info("ccai -> ucp incremental started")
        df_profile.cache()
        profile_count_init = df_profile.count()
        observed_passengers = df_profile.filter("(FirstName is not null) and (LastName is not null) and (DateOfBirth is not null) and (DateOfBirth>='1947') and (DateOfBirth not like '%xxx%')")\
                                       .select(F.lower(F.regexp_replace(unidecode_encode("FirstName"), "[^a-zA-Z0-9 ]", "")).alias("FirstName"),
                                               F.lower(F.regexp_replace(unidecode_encode("LastName"), "[^a-zA-Z0-9 ]", "")).alias("LastName"),
                                               F.col("DateOfBirth").alias("DateOfBirth"),
                                               F.col("passenger_hash").alias("passenger_hash"),
                                               F.col("ProvisionalPrimaryKey").alias("ProvisionalPrimaryKey"))\
                                       .filter("(FirstName not in ('{}')) and (LastName not in ('{}'))".format("','".join(firstname_filters), "','".join(lastname_filters)))\
                                       .withColumn("pFirstName",F.trim(F.regexp_replace(F.regexp_replace(F.col("FirstName"), "\.", " "), "^(mr|ms|dr|rev|prof|sir|madam|miss|mrs|st)", "")))\
                                       .withColumn("pFirstName", F.when(F.col("DateOfBirth").like("9999%"),F.lower("pFirstName")).otherwise(F.trim(phonetic_encode_udf(F.split(F.lower("pFirstName"), " ").getItem(0)))))\
                                       .withColumn("pLastName", F.when(F.col("DateOfBirth").like("9999%"), F.lower("LastName")).otherwise(F.trim(F.lower(phonetic_encode_udf(F.col("LastName")))))) \
                                       .select("ProvisionalPrimaryKey","pFirstName","pLastName","DateOfBirth","passenger_hash")#.dropDuplicates()
        observed_passengers.cache()
        old_passengers = spark.read.option("basePath",new_passengers_base_path).parquet(*get_old_passengers_paths(new_passengers_base_path,day0_date,end_date)).drop("p_date").withColumnRenamed("DateOfBirth","DateOfBirth_r").withColumnRenamed("passenger_hash","passenger_hash_r")

        retained_passengers = old_passengers.join(observed_passengers, ["pFirstName","pLastName"])\
                                            .withColumn("dateSim", get_date_similarity(F.col("DateOfBirth"), F.col("DateOfBirth_r")))\
                                            .filter("dateSim").drop("dateSim")
        retained_passengers.cache()
        
        passenger_hash_overrides = retained_passengers.select("passenger_hash","passenger_hash_r").groupBy("passenger_hash").agg(F.max("passenger_hash_r").alias("passenger_hash_r"))#.dropDuplicates()#.cache()
        passenger_hash_overrides.cache()
        
        new_passengers = observed_passengers.join(retained_passengers, "ProvisionalPrimaryKey", "left_anti").drop("ProvisionalPrimaryKey").dropDuplicates()
        new_passengers.cache()
        LOGGER.info("ccai -> Num New Passengers Rows/Records : "+ str(new_passengers.count()))
        df_profile.join(passenger_hash_overrides, ["passenger_hash"], "left").withColumn("passenger_hash", F.coalesce("passenger_hash_r","passenger_hash")).drop("passenger_hash_r").write.parquet(ucp_path, mode="overwrite")
        new_passengers.write.parquet(new_passengers_base_path+"/p_date={}".format(partition_date), mode='overwrite')

        df_profile_out = spark.read.parquet(ucp_path)
        df_profile_out.cache()
        LOGGER.info("ccai -> Num Rows Dedupe : " + str(num_rows_dedupe))
        LOGGER.info("ccai -> Num Rows Input : " + str(num_rows_in))
        LOGGER.info("ccai -> Num Rows Output : " + str(df_profile_out.count()))
        LOGGER.info("ccai -> Num fn, ln, dob Combinations : " + str(df_profile_out.select("FirstName","LastName","DateOfBirth").dropDuplicates().count()))
        LOGGER.info("ccai -> Num Passenger Hash : " + str( df_profile_out.select("passenger_hash").dropDuplicates().count()))
        LOGGER.info("ccai -> Num Rows Without Hash : " + str(df_profile_out.filter("passenger_hash is null").count()))
        ambiguous_hash = df_profile_out.groupBy("FirstName","LastName","DateOfBirth").agg(F.countDistinct("passenger_hash").alias("numHash")).filter("numHash>1")
        ambiguous_hash.cache()
        LOGGER.info("ccai -> Num Ambiguous Hashes : " + str(ambiguous_hash.count()))
        LOGGER.info("ccai -> Ambiguous Hash Samples : \n" + str( ambiguous_hash.orderBy(F.desc("numHash")).limit(10).toPandas().to_csv(index=False)))
        LOGGER.info("ccai -> ucp done")


    return ucp_path
