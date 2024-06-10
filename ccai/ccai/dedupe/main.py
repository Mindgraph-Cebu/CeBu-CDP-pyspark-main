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

def get_config(file_path):
    # file_path = "s3a://cebu-cdp-data-dev/script/glue/cebu-cdp-profile-glue/profile_config_cebu_v1.json"
    bucket_name = file_path.split("/")[2]
    key = "/".join(file_path.split("/")[3:])
    s3 = boto3.client("s3")
    config = json.loads(s3.get_object(Bucket=bucket_name, Key=key)["Body"].read())
    return config


def reverseFillNa(config, profile_df, spark):
    # iterate on profile_df columns
    for column_name in profile_df.columns:
        for each_column in config["profileSchema"]:
            if each_column["columnName"] == column_name:
                # replace defaultValue with None
                profile_df = profile_df.withColumn(
                    each_column["columnName"],
                    F.when(
                        F.col(each_column["columnName"]) == each_column["defaultValue"],
                        "CCAI_NULL",
                    ).otherwise(F.col(each_column["columnName"])),
                )

    return profile_df



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

class DriverLogs:
    def __init__(self, log_text = "Dedupe Starts!"):
        self.checkpoint_count = 1
        self.log_list = list()
        self.log_list.append("{} : Checkpoint-{:03d} -> {}".format(dt.now(),self.checkpoint_count, log_text))
    
    def log(self, log_text=''):
        self.checkpoint_count+=1
        self.log_list.append("{} : Checkpoint-{:03d} -> {}".format(dt.now(),self.checkpoint_count, log_text))

    def print_logs(self):
        print("*"*100)
        print("Driver Logs: ")
        print("\n".join(self.log_list))

    def return_logs(self):
        return """\n{}\n{}\n{}\n{}""".format("*"*100, "Driver Logs: ", "\n".join(self.log_list), "*"*100)


def compute_dedupe(config_path, spark, end_date, LOGGER, loaded_dob_graph):
    partition_date = end_date
    driver_log = DriverLogs()
    config = get_config(config_path)
    spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    num_cores = spark.sparkContext.defaultParallelism

    spark.conf.set("spark.sql.shuffle.partitions", num_cores*2)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.files.maxRecordsPerFile", 100000)
    spark.conf.set("spark.sql.files.maxPartitionBytes", 52428800)
    # spark.conf.set("spark.sql.shuffle.partitions", "400")
    # increase default parallelism
    # spark.conf.set("spark.default.parallelism", "400")

    # spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # spark.conf.set("spark.sql.shuffle.partitions", "1000")
    # spark.conf.set("spark.default.parallelism", "1000")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    # spark.conf.set("spark.executor.memory", "8g")
    # df = spark.read.load(config['storageDetails'][0]['pathUrl'], format="parquet").select("ProvisionalPrimaryKey","FirstName", "LastName", "DateOfBirth", "Gender","EmailAddress", "Phone", "PassengerID", "PersonID", "BookerFirstName", "BookerLastName")
    df = spark.read.load(
        config["storageDetails"][0]["pathUrl"]+"/p_date="+partition_date, format="parquet"
    ).select(
        "ProvisionalPrimaryKey",
        "FirstName",
        "LastName",
        "DateOfBirth",
        "Gender",
        "PassengerID",
    )
    df = df.withColumn(
        "FirstName",
        F.when(
            F.col("FirstName") == "Unknown",
            "CCAI_NULL",
        ).otherwise(F.col("FirstName")),
    )
    df = reverseFillNa(config, df, spark)
    # df_booker = df.select("ProvisionalPrimaryKey","BookerFirstName", "BookerLastName", "PassengerID", "PersonID")
    CCAI_NULL = "CCAI_NULL"

    # ! make FNU in FirstName column as null
    # df = df.withColumn("FirstName", F.when(F.col("FirstName") == "FNU", None).otherwise(F.col("FirstName")))
    # make DateOfBirth as null if first four elements is less than 1947
    df = df.withColumn(
        "DateOfBirth",
        F.when(F.col("DateOfBirth").substr(0, 4) < "1947", CCAI_NULL).otherwise(
            F.col("DateOfBirth")
        ),
    )
    # df = df.withColumn(
    #     "DateOfBirth",
    #     F.when(F.col("DateOfBirth").substr(0, 4) > "3000", CCAI_NULL).otherwise(
    #         F.col("DateOfBirth")
    #     ),
    # )
    # df = df.withColumn(
    #     "DateOfBirth",
    #     F.when(F.col("DateOfBirth").like("9999%"), CCAI_NULL).otherwise(
    #         F.col("DateOfBirth")
    #     ),
    # )
    df = df.withColumn(
        "DateOfBirth",
        F.when(F.col("DateOfBirth").like("%xxx%"), CCAI_NULL).otherwise(
            F.col("DateOfBirth")
        ),
    )

    def unidecode_encode(name):
        try:
            return unidecode(name)
        except:
            return "xxx"

    df = df.withColumn(
        "FirstName", F.udf(unidecode_encode, T.StringType())(F.col("FirstName"))
    )
    df = df.withColumn(
        "LastName", F.udf(unidecode_encode, T.StringType())(F.col("LastName"))
    )

    # convert FirstName, LastName to lower case, remove spaces and special characters
    df = df.withColumn(
        "FirstName", F.lower(F.regexp_replace(F.col("FirstName"), "[^a-zA-Z0-9 ]", ""))
    )
    df = df.withColumn(
        "FilterFirstName",
        F.lower(F.regexp_replace(F.col("FirstName"), "[^a-zA-Z0-9]", "")),
    )
    df = df.withColumn(
        "LastName", F.lower(F.regexp_replace(F.col("LastName"), "[^a-zA-Z0-9]", ""))
    )
    df = df.withColumn(
        "FilterLastName",
        F.lower(F.regexp_replace(F.col("LastName"), "[^a-zA-Z0-9]", "")),
    ).cache()

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
    ]

    driver_log.log("Initial number of Rows = {}".format(df.count()))
    LOGGER.info("ccai - Initial number of Rows = {}".format(df.count()))
    df = df.filter(
        ~F.col("FilterFirstName").isin(*firstname_filters)
        & ~F.col("FilterLastName").isin(*lastname_filters)
    )
    driver_log.log("Number of Rows after filtering firstname and lastname = {}".format(df.count()))
    LOGGER.info("ccai - Number of Rows after filtering firstname and lastname = {}".format(df.count()))

    df = df.drop(*("FilterFirstName", "FilterLastName"))
    df = df.filter(F.col("PassengerID").isNotNull())

    driver_log.log("Number of Rows after removing null passengerId = {}".format(df.count()))
    LOGGER.info("ccai - Number of Rows after removing null passengerId = {}".format(df.count()))
    def pcolumn_hashing(pFirstName, pLastName):
        return hashlib.md5(
            str(pFirstName).encode("utf-8")
            + str(pLastName).encode("utf-8")
            # + str(dob).encode("utf-8")
        ).hexdigest()
        # return hashlib.md5(str(pLastName).encode("utf-8")).hexdigest()
        # return str(pFirstName) + str(pLastName)

    sm = SpanishMetaphone()

    def phonetic_encode(sm, name):
        try:
            if len(str(name)) > 3:
                return sm.encode(name)
            else:
                return str(name)
        except:
            return "CCAI_NULL"

    # clean first name such as replacing . with space
    df = df.withColumn("pFirstName", F.regexp_replace(F.col("FirstName"), "\.", " "))
    # remove prefix such as Mr. Mrs. Ms. Dr. Rev. etc.
    df = df.withColumn(
        "pFirstName",
        F.regexp_replace(
            F.col("pFirstName"), "^(mr|ms|dr|rev|prof|sir|madam|miss|mrs|st)", ""
        ),
    )
    df = df.withColumn("pFirstName", F.trim(F.col("pFirstName")))
    # F.when(F.col("DateOfBirth").like("9999%"), CCAI_NULL).otherwise()
    phonetic_encode_udf = F.udf(lambda x: phonetic_encode(sm, x), T.StringType())
    # df = df.withColumn(
    #     "pFirstName",
    #     F.when(F.col("DateOfBirth").like("9999%"), F.lower("pFirstName")).otherwise(F.trim( F.split(phonetic_encode_udf(F.lower("pFirstName"))).getItem(0) ))
    # )


    df = df.withColumn(
    "pFirstName",
    F.when(
        F.col("DateOfBirth").like("9999%"),
        F.lower("pFirstName")
    ).otherwise(
        F.trim(phonetic_encode_udf(F.split(F.lower("pFirstName"), " ").getItem(0)))
    ))

    #commented to check - Vietnamese names
    # df = df.withColumn(
    # "pFirstName",
    # F.when(
    #     (F.col("DateOfBirth").like("9999%")) | (F.length(F.split(F.lower("pFirstName"), " ").getItem(0)) < 4),
    #     F.lower("pFirstName")
    # ).otherwise(
    #     F.trim(phonetic_encode_udf(F.split(F.lower("pFirstName"), " ").getItem(0)))
    # )
    # )


    # df = df.withColumn("pFirstName", F.trim( F.split(F.col("pFirstName"), " ").getItem(0) ))
    df = df.withColumn(
        "pLastName",
        F.when(F.col("DateOfBirth").like("9999%"), F.lower("LastName")).otherwise(F.trim(phonetic_encode_udf(F.lower("LastName"))))
    )
    # df_booker = df_booker.repartition(200)
    # df_booker = df_booker.withColumn("pBookerFirstName", F.lower(F.regexp_replace(F.col("BookerFirstName"), "[^a-zA-Z0-9]", "")))
    # df_booker = df_booker.withColumn("pBookerLastName", F.lower(F.regexp_replace(F.col("BookerLastName"), "[^a-zA-Z0-9]", "")))
    # df_booker = df_booker.withColumn("pBookerFirstName", F.udf(lambda x: phonetic_encode(sm,x), T.StringType())(F.col("pBookerFirstName")))
    # df_booker = df_booker.withColumn("pBookerLastName", F.udf(lambda x: phonetic_encode(sm,x), T.StringType())(F.col("pBookerLastName")))
    # # apply pcolumn_hashing to FirstName and LastName
    # df_booker = df_booker.withColumn("booker_hash", F.udf(pcolumn_hashing, T.StringType())(F.col("pBookerFirstName"), F.col("pBookerLastName")))
    # # fill nulls with ccai_null
    # df = df.fillna(CCAI_NULL)
    #! Check
    # df = df.withColumn("PassengerID",F.col("PassengerID").cast(T.StringType()))
    # df = df.withColumn("PersonID",F.col("PersonID").cast(T.StringType()))
    # df = df.withColumn("pFirstName", ceja.match_rating_codex(F.col("FirstName"))).withColumn("pLastName", ceja.match_rating_codex(F.col("LastName")))
    email_regex = """(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"""

    # df = df.withColumn("EmailAddress",
    #                     F.when(
    #                         F.col("EmailAddress").rlike(email_regex),
    #                         F.col("EmailAddress")
    #                     ).otherwise(CCAI_NULL)
    #                 )
    # df = df.withColumn("FirstName", F.when(F.col("FirstName").isNull(), CCAI_NULL).otherwise(F.col("FirstName")))
    df = df.withColumn(
        "LastName",
        F.when(F.col("LastName").isNull(), CCAI_NULL).otherwise(F.col("LastName")),
    )
    df = df.withColumn(
        "DateOfBirth",
        F.when(F.col("DateOfBirth").isNull(), CCAI_NULL).otherwise(
            F.col("DateOfBirth")
        ),
    )
    # df = df.withColumn("EmailAddress", F.when(F.col("EmailAddress").isNull(), CCAI_NULL).otherwise(F.col("EmailAddress")))
    # df = df.withColumn("Phone", F.when(F.col("Phone").isNull(), CCAI_NULL).otherwise(F.col("Phone")))
    df = df.withColumn(
        "Gender", F.when(F.col("Gender").isNull(), CCAI_NULL).otherwise(F.col("Gender"))
    )
    df = df.withColumn(
        "ProvisionalPrimaryKey",
        F.when(F.col("ProvisionalPrimaryKey").isNull(), CCAI_NULL).otherwise(
            F.col("ProvisionalPrimaryKey")
        ),
    )
    df_count = df.count()
    LOGGER.info("ccai dob cluster final length: {}".format(df_count))
    driver_log.log("ccai dob cluster final length = {}".format(df_count))



    @udf()
    def generate_random_string():
        return "".join(random.choices(string.ascii_letters + string.digits, k=27))

    def pcolumn_hashing(pFirstName, pLastName):
        return hashlib.md5(
            str(pFirstName).encode("utf-8")
            + str(pLastName).encode("utf-8")
            # + str(dob).encode("utf-8")
        ).hexdigest()

    df = df.withColumn(
        "group_hash",
        F.udf(pcolumn_hashing, T.StringType())(
            # F.col("pFirstName"), F.col("pLastName"), F.col("dob_hash")
            F.col("pfirstname"),
            F.col("plastname"),
            # F.col("dob_hash"),
        ),
    )
    # df = df.withColumn("dob_count", F.size(F.col("DateOfBirth")))
    # df_single = df.filter(F.col("dob_count") == 1)
    # df = df.filter(F.col("dob_count") > 1)

    df = df.fillna("FNU", subset="firstname")
    replacement_expr = " junior alias son of $1"
    df = df.withColumn(
        "firstname",
        F.regexp_replace(F.col("FirstName"), "^(jr|jr.)", " junior alias son of $1"),
    ).cache()

    df_count = df.count()
    driver_log.log("Row Count at next Checkpoint = {}".format(df_count))
    driver_log.log("Row Count - Null DOB Rows = {}".format(df.filter("dateofbirth = 'CCAI_NULL'").count()))

    LOGGER.info("ccai - Row Count at next Checkpoint = {}".format(df_count))
    LOGGER.info("ccai - Row Count - Null DOB Rows = {}".format(df.filter("dateofbirth = 'CCAI_NULL'").count()))

    df.createOrReplaceTempView("df_dedupe")
    print(
        "num of dob nulls ",
        spark.sql(
            "select count(*) as cnt from df_dedupe where dateofbirth = 'CCAI_NULL'"
        ).show(),
    )
    # df.createOrReplaceTempView("df_dedupe")
    df = spark.sql(
        """
    select firstname, lastname, passengerid, pfirstname,plastname, provisionalprimarykey, group_hash,
    case when dateofbirth = 'CCAI_NULL' then null else dateofbirth end as dateofbirth from df_dedupe
    """
    )

    dob_null_handler = """
    SELECT
        t1.firstname,
        t1.lastname,
        t1.passengerid,
        t1.pfirstname,
        t1.plastname,
        t1.group_hash,
        t1.provisionalprimarykey,
        COALESCE(t1.dateofbirth, t2.dateofbirth) AS dateofbirth
    FROM
        df_dedupe t1
    left join (
        select firstname, lastname, dateofbirth from (
            select firstname, lastname, dateofbirth from df_dedupe where dateofbirth is not null and firstname in (select firstname from df_dedupe where dateofbirth == 'CCAI_NULL') and lastname in (select lastname from df_dedupe where dateofbirth == 'CCAI_NULL')
            ) group by firstname, lastname, dateofbirth
        ) as t2 on t1.firstname = t2.firstname and t1.lastname = t2.lastname
    """

    # dob_null_handler_new = "select firstname, lastname, max(dateofbirth) as dateofbirth_na from df_dedupe where (dateofbirth is not null) and (dateofbirth!='CCAI_NULL') and (dateofbirth not like '9999%') and (dateofbirth>='1947-01-01') and (dateofbirth<'2023-12-12') group by firstname, lastname"
    dob_null_handler_new = "select firstname, lastname, max(dateofbirth) as dateofbirth_na from df_dedupe where (dateofbirth is not null) and (dateofbirth!='CCAI_NULL') and (dateofbirth>='1947-01-01') and (dateofbirth<'2023-12-12') group by firstname, lastname"
    # df.createOrReplaceTempView("df_dedupe")
    df = df.join(spark.sql(dob_null_handler_new), ['firstname', 'lastname'], "left").withColumn("dateofbirth",F.coalesce("dateofbirth","dateofbirth_na")).drop("dateofbirth_na")
    from pyspark.sql.window import Window

    window_spec = Window.partitionBy(["firstname", "lastname"])
    df = df.withColumn("dateofbirthfilled", F.max("dateofbirth").over(window_spec))
    df = df.withColumn(
        "dateofbirth",
        F.when(F.col("dateofbirth").isNull(), F.col("dateofbirthfilled")).otherwise(
            F.col("dateofbirth")
        ),
    )
    # print("dob nulls handled ", df.count())

    drop_nulls = """
    select * from df_dedupe where dateofbirth is not NULL
    """
    # df.createOrReplaceTempView("df_dedupe")
    df = spark.sql(drop_nulls).cache()
    df_count=df.count()
    driver_log.log("dropping unresolved dob nulls = {}".format(df_count))
    LOGGER.info("ccai - dropping unresolved dob nulls = {}".format(df_count))

    groupby = """
    SELECT
        firstname,
        lastname,
        dateofbirth,
        first(group_hash) as group_hash,
        collect_set(provisionalprimarykey) AS passengerid_list,
        first(passengerid) as passengerid,
        first(provisionalprimarykey) as provisionalprimarykey,
        first(pfirstname) as pfirstname,
        first(plastname) as plastname
    FROM
        df_dedupe
    GROUP BY
        firstname,
        lastname,
        dateofbirth;
    """
    df.createOrReplaceTempView("df_dedupe")
    df = spark.sql(groupby)
    df.cache()
    df_count=df.count()
    driver_log.log("dropping unresolved dob nulls = {}".format(df_count))
    LOGGER.info("ccai - dropping unresolved dob nulls = {}".format(df_count))
    disp_str=df.withColumn("l_size", F.size("passengerid_list")).drop("passengerid_list").orderBy(F.desc("l_size")).limit(5).toPandas().to_csv(index=False)
    LOGGER.info("ccai - ********************************************")
    LOGGER.info("ccai - dropping unresolved dob nulls df Largest list:\n{}".format(str(disp_str)))

    edge_case_condition = "( (pfirstname in ('TV')) and (plastname in ('TV','PX')) )"
    ignored_profiles = df.filter(edge_case_condition)
    df = df.filter("not " + edge_case_condition)
    ignored_profiles.write.mode("overwrite").parquet(config["storageDetails"][4]["pathUrl"])
    disp_str = spark.read.parquet(config["storageDetails"][4]["pathUrl"]).limit(10).drop("passengerid_list").toPandas().to_csv(index=False)
    LOGGER.info("ccai - *************************************")
    LOGGER.info("ccai - ignored profiles rows df count : " + str (df.count()))
    LOGGER.info("ccai - ignored profiles rows : " + str (spark.read.parquet(config["storageDetails"][4]["pathUrl"]).count()))
    LOGGER.info("ccai - df -> "+str(disp_str))

    crossjoin = """
    select t1.*,
    t2.firstname as firstname_right, 
    t2.lastname as lastname_right, 
    t2.dateofbirth as dateofbirth_right, 
    t2.passengerid as passengerid_right, 
    t2.provisionalprimarykey as provisionalprimarykey_right,
    t2.passengerid_list as passengerid_list_right, 
    t2.group_hash as group_hash_right 
    from df_dedupe as t1 left join df_dedupe as t2 on t1.passengerid != t2.passengerid and t1.pfirstname == t2.pfirstname and t1.plastname == t2.plastname
    """
    df.createOrReplaceTempView("df_dedupe")
    df = (spark.sql(crossjoin)
          .withColumn("dateSim", get_date_similarity(F.col("dateofbirth"), F.col("dateofbirth_right")))
          .filter("dateSim").drop("dateSim"))
    df.cache()
    df_count=df.count()
    driver_log.log("Num Rows After Cross Join = {}".format(df_count))
    LOGGER.info("ccai - Num Rows After Cross Join = {}".format(df_count))
    # df.show()

    df = df.withColumn(
        "firstnamesim",
        F.when(
            (F.col("firstname") != "CCAI_NULL")
            & (F.col("firstname_right") != "CCAI_NULL"),
            ceja.jaro_winkler_similarity(F.col("firstname"), F.col("firstname_right")),
        ).otherwise(0),
    )
    df = df.withColumn(
        "lastnamesim",
        F.when(
            (F.col("lastname") != "CCAI_NULL")
            & (F.col("lastname_right") != "CCAI_NULL"),
            ceja.jaro_winkler_similarity(F.col("lastname"), F.col("lastname_right")),
        ).otherwise(0),
    )

    # condition = (F.col("firstnamesim") >= 0.85) & (F.col("lastnamesim") >= 0.90)
    # df = df.filter(condition)
    df = df.withColumn(
        "passengerid_list",
        F.when(
            (F.col("firstnamesim") >= 0.85) & (F.col("lastnamesim") >= 0.90),
            F.array_distinct(F.concat(F.col("passengerid_list"), F.col("passengerid_list_right"))),
        ).otherwise(F.array_distinct(F.col("passengerid_list"))),
    ).dropDuplicates().cache()
    df_count=df.count()
    driver_log.log("condition based concat = {}".format(df_count))
    LOGGER.info("ccai - condition based concat = {}".format(df_count))
    LOGGER.info("ccai - condition based concat df:\n{}".format(df.limit(5).toPandas().to_csv(index=False)))
    disp_str1 = df.withColumn("l_size", F.size("passengerid_list")).drop("passengerid_list","passengerid_list_right").orderBy(F.desc("l_size")).limit(5).toPandas().to_csv(index=False)
    disp_str2 = df.withColumn("r_size", F.size("passengerid_list_right")).drop("passengerid_list","passengerid_list_right").orderBy(F.desc("r_size")).limit(5).toPandas().to_csv(index=False)
    LOGGER.info("ccai - ********************************************")
    LOGGER.info("ccai - condition based concat df Largest list:\n{}".format(str(disp_str1+"\nccai\n"+disp_str2)))
    disp_str3 = df.withColumn("l_size", F.expr("size(array_union(passengerid_list,passengerid_list_right))")).groupBy("pfirstname", "plastname").agg(F.sum("l_size").alias("l_size")).orderBy(F.desc("l_size")).limit(5).toPandas().to_csv(index=False)
    LOGGER.info("ccai - ********************************************")
    LOGGER.info("ccai - after grouping phoetic name- Largest list:\nccai - {}".format(str(disp_str3)))

    def cc_nx_udf(df: pd.DataFrame) -> pd.DataFrame:
        group_hash = df["group_hash"].iloc[0]
        dob_tag_list = df["passengerid_list"].to_list()

        def to_graph(l):
            G = networkx.Graph()
            for part in l:
                # each sublist is a bunch of nodes
                G.add_nodes_from(part)
                # it also imlies a number of edges:
                G.add_edges_from(to_edges(part))
            return G

        def to_edges(l):
            """
            treat `l` as a Graph and returns it's edges
            to_edges(['a','b','c','d']) -> [(a,b), (b,c),(c,d)]
            """
            it = iter(l)
            last = next(it)

            for current in it:
                yield last, current
                last = current
        try:
            G = to_graph(dob_tag_list)
            cluster = [list(x) for x in list(connected_components(G))]
            dob = list()
            dob_hash = list()
            gp_list = list()
            for ind, each in enumerate(cluster):
                dob_hash_first_element = hashlib.md5(str(each[0]).encode("utf-8")).hexdigest()
                for each_element in each:
                    dob.append(each_element)
                    gp_list.append(group_hash)
                    dob_hash.append(dob_hash_first_element)
            result_df = pd.DataFrame(
                {
                    "group_hash": gp_list,
                    "passenger_hash": dob_hash,
                    "provisionalprimarykey": dob
                }
            )
            return result_df
        except:
            dob =  dob_tag_list[0]# list(set([i for l in dob_tag_list for i in l]))
            gp_list = [group_hash for i in dob]
            dob_hash = ['_error_val_' for i in dob]
            return pd.DataFrame({
                    "group_hash": gp_list,
                    "passenger_hash": dob_hash,
                    "provisionalprimarykey": dob
                })

    ccSchema = T.StructType(
        [
            T.StructField("group_hash", T.StringType()),
            T.StructField("provisionalprimarykey", T.StringType()),
            T.StructField("passenger_hash", T.StringType()),
        ]
    )
    df = df.groupby(["pfirstname", "plastname"]).applyInPandas(
        cc_nx_udf, schema=ccSchema
    ).cache()
    df_count=df.count()
    driver_log.log("Final Count = {}".format(df_count))
    LOGGER.info("ccai - Final Count = {}".format(df_count))
    LOGGER.info("ccai - Final Count (Error - unassigned) = {}".format(df.filter("passenger_hash like '_error_val_'")))

    # df.write.mode("overwrite").parquet("s3a://cebu-cdp-data-dev/dedupe-cluster-1")
    LOGGER.info("ccai write: {}".format(df_count))
    save_path = config["storageDetails"][1]["pathUrl"]+ \
        "/" + "p_date=" + partition_date
    driver_log.log("Save Path = {}".format(save_path))

    # df.drop("pFirstName", "pLastName").write.mode("overwrite").parquet(save_path)
    df.write.mode("overwrite").parquet(save_path)
    # driver_log.print_logs()
    LOGGER.info(driver_log.return_logs())
    dedupe_df_10rowsStr = spark.read.parquet(save_path).limit(10).toPandas().to_csv(index=False)
    LOGGER.info("ccai - sample 10 rows : \n"+ str(dedupe_df_10rowsStr))
    # df.write.mode("overwrite").parquet("s3a://cebu-cdp-data-dev/dedupe-cluster-1")
    return save_path
