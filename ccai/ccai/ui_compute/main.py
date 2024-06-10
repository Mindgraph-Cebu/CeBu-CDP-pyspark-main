import pyspark
from pyspark.sql.functions import udf, col, size, collect_list, collect_list
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from collections import Counter
from pyspark.sql.functions import (
    col,
    collect_list,
    struct,
    count,
    when,
    greatest,
    date_format,
    expr,
    current_date,
)
from pyspark.sql.types import StructType, StructField, IntegerType
from datetime import date
from datetime import datetime
import statistics
import json
import pandas as pd
from pyspark.sql.functions import year, when, floor
import boto3

def get_paths(day0_date,end_date,path,LOGGER):
    bucket_name = path.split("/")[2]
    prefix = "ucp/"
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

    
    paths = []
    for common_prefix in response.get('CommonPrefixes', []):
        folder_name = common_prefix['Prefix']
        date_str = folder_name.split("=")[-1].rstrip('/')
        if day0_date <= date_str <= end_date:
            s3_path = 's3a://{}/ucp/p_date={}/'.format(bucket_name,date_str)
            paths.append(s3_path)
    return paths


def get_config(file_path):
    # file_path = "s3a://cebu-cdp-data-qa/script/glue/cebu-cdp-profile-glue/profile_config_cebu_v1.json"
    bucket_name = file_path.split("/")[2]
    key = "/".join(file_path.split("/")[3:])
    s3 = boto3.client("s3")
    config = json.loads(s3.get_object(Bucket=bucket_name, Key=key)["Body"].read())
    return config


@udf()
def most_travelled(my_list):
    new_list = ["Unknown" if x == "" else x for x in my_list]
    count_dict = dict(Counter(new_list))
    result_dict = {k: count_dict[k] for i, k in enumerate(count_dict) if i < 50}
    json_string = json.dumps(result_dict)
    return json_string

@udf()
def most_travelledOrigin(my_list):
    result_dict = dict()
    for row in my_list:
        k = row['TravelOrigin']
        v = row['numRows']
        k = 'Unknown' if k is None else ('Unknown' if k.strip()=='' else k)
        if k not in result_dict.keys():
            result_dict[k]=0
        result_dict[k]+=v
    return json.dumps(result_dict)

@udf()
def most_travelledSeat(my_list):
    result_dict = dict()
    for row in my_list:
        k = row['TravelSeat']
        v = row['numRows']
        k = 'Unknown' if k is None else ('Unknown' if k.strip()=='' else k)
        if k not in result_dict.keys():
            result_dict[k]=0
        result_dict[k]+=v
    return json.dumps(result_dict)

@udf()
def most_travelledDestination(my_list):
    result_dict = dict()
    for row in my_list:
        k = row['TravelDestination']
        v = row['numRows']
        k = 'Unknown' if k is None else ('Unknown' if k.strip()=='' else k)
        if k not in result_dict.keys():
            result_dict[k]=0
        result_dict[k]+=v
    return json.dumps(result_dict)

@udf()
def booker_timeline(
    Revenue,
    TravelDate,
    BookingDate,
    TravelOrigin,
    TravelDestination,
    TravelInsurance,
    TravelBaggage,
    TravelMeals,
    BookingCurrency,
    RecordLocator,
):
    timeline = {}
    for index in range(len(TravelDate)):
        timeline[index] = {
            "origin": TravelOrigin[index],
            "destination": TravelDestination[index],
            "insurance": TravelInsurance[index],
            "baggage": TravelBaggage[index],
            "meals": TravelMeals[index],
            "travel_date": TravelDate[index].split(" ")[0],
            "booking_date": BookingDate[index].split(" ")[0],
            "revenue": Revenue[index],
            "currency": BookingCurrency[index],
            "record_locator":RecordLocator[index]
        }
    json_string = json.dumps(timeline)
    return json_string

@udf()
def booker_timeline_new(rows):
    return_val = dict()
    for idx, row in enumerate(rows):
        return_val[idx] = row
    return json.dumps(return_val)

@udf()
def passenger_timeline_new(rows):
    return_val = dict()
    for idx, row in enumerate(rows):
        return_val[idx] = row
    return json.dumps(return_val)


@udf()
def passenger_timeline(
    Revenue,
    TravelDate,
    BookingDate,
    TravelOrigin,
    TravelDestination,
    TravelInsurance,
    TravelBaggage,
    TravelMeals,
    RecordLocator,
):
    timeline = {}
    for index in range(len(TravelDate)):
        timeline[index] = {
            "origin": TravelOrigin[index],
            "destination": TravelDestination[index],
            "insurance": TravelInsurance[index],
            "baggage": TravelBaggage[index],
            "meals": TravelMeals[index],
            "travel_date": TravelDate[index].split(" ")[0],
            "booking_date":BookingDate[index].split(" ")[0],
            "revenue": Revenue[index],
            "record_locator":RecordLocator[index]
            # 'currency':BookingCurrency[index]
        }
    json_string = json.dumps(timeline)
    return json_string


def booker_compute(spark, df, save_path_prefix, LOGGER):
    lst = [
        "BookingMonth",
        "TravelMeals",
        "BookingChannel",
        "IsEmployee",
        "TravelBaggage",
        "Gender",
        "IsEmployeeDependent",
        "TravelInsurance",
        "TravelSoloOrGroup",
        "IsRegistered",
        "BookingCurrency",
        "AgeRange",
    ]

    dfs = {}
    dfs1 = {}
    aggFunc = {
        "BookerFirstName": F.max("BookerFirstName"),
        "BookerLastName": F.max("BookerLastName"),
        "BookerEmailAddress": F.max("BookerEmailAddress"),
        "BookerMobile": F.max("BookerMobile"),
        "BookerType": F.max("BookerType"),
        "BookerCity": F.max("BookerCity"),
        "LatestDate": F.max("LatestDate"),
        "FirstDate": F.min("FirstDate"),
        #"UniqueClients": F.size(F.array_distinct(F.flatten(F.collect_list("UniqueClients"))))
    }
    # COLLECT_SET(passenger_hash)
    # AS
    # UniqueClients,
    result0 = spark.sql("select PersonID, count (distinct (passenger_hash)) as UniqueClients from cdp_profiles group by PersonID")
    result0.cache()
    checkpoint_str = result0.orderBy(F.desc("UniqueClients")).limit(5).toPandas().to_csv(index=False)
    LOGGER.info("bookerccai checkpoint-1 : \n" + str(checkpoint_str))
    query = """SELECT PersonID,
                      TravelOrigin,
                      TravelDestination,
                      TravelSeat,
                      count(*) as numRows,
                      MAX(BookerFirstName) AS BookerFirstName,
                      MAX(BookerLastName) AS BookerLastName,
                      MAX(BookerEmailAddress) AS BookerEmailAddress,
                      MAX(BookerMobile) AS BookerMobile,
                      SUM(New_Revenue) AS TotalRevenue,
                      MAX(BookerType) AS BookerType,
                      MAX(BookerCity) AS BookerCity,
                      Max(Bookingdate) AS LatestDate,
                      MIN(Bookingdate) AS FirstDate, 
                      COUNT(PassengerID) AS TotalPassengers"""
    for i in lst:
        dfs[i] = [row[0] for row in df.select(i).distinct().collect()]
        LOGGER.info("bookerccai - Distincts {} -> {}".format(i,len(dfs[i])))
        dfs1[i] = [
            word.replace(" ", "_space1_")
            .replace("-", "_space2_")
            .replace("(", "_space3_")
            .replace(")", "_space4_")
            for word in dfs[i]
        ]
        for j, k in zip(dfs[i], dfs1[i]):
            query += (
                ",SUM(CASE WHEN "
                + i
                + ' = "'
                + j
                + '" THEN 1 ELSE 0 END) AS '
                + i
                + "_separator_"
                + k
                + " "
            )

    spark.udf.register("most_travelled", most_travelled)
    #query += ",most_travelled(collect_list(TravelOrigin)) AS TravelOrigin,most_travelled(collect_list(TravelDestination)) AS TravelDestination,most_travelled(collect_list(TravelSeat)) AS TravelSeat"
    query += " FROM cdp_profiles GROUP BY PersonID,TravelOrigin,TravelDestination,TravelSeat"
    # print(query)

    checkpoint_str = spark.sql("select PersonID, count(*) as cnt from cdp_profiles group by PersonID").orderBy(F.desc("cnt")).limit(5).toPandas().to_csv(index=False)
    LOGGER.info("bookerccai checkpoint-1 : \n"+str(checkpoint_str))
    result1_pre = spark.sql(query)
    result1_pre.cache()
    cols_result1_pre = list(result1_pre.columns)[5:]

    LOGGER.info("bookerccai checkpoint-1.5 -> result1_pre count : "+str(result1_pre.count()))

    result1 = result1_pre.groupBy("PersonID").agg(
        *[aggFunc.get(colName,F.sum(colName)).alias(colName) for colName in cols_result1_pre],
        most_travelledOrigin(F.collect_list(F.struct("TravelOrigin","numRows"))).alias("TravelOrigin"),
        most_travelledDestination(F.collect_list(F.struct("TravelDestination", "numRows"))).alias("TravelDestination"),
        most_travelledSeat(F.collect_list(F.struct("TravelSeat", "numRows"))).alias("TravelSeat")
    ).join(result0, ['PersonID'],"left")

    result1.cache()
    LOGGER.info("bookerccai checkpoint-2 -> Result1 count : "+str(result1.count()))
    LOGGER.info("bookerccai checkpoint-2 -> Result1 df sample : \n"+str(result1.limit(5).toPandas().to_csv(index=False)))



    result2 = spark.sql(
        """
    SELECT PersonID, 
            booker_timeline_new(collect_list(
            from_json(to_json(struct(TravelOrigin as origin,
                                TravelDestination as destination,
                                TravelInsurance as insurance,
                                TravelBaggage as baggage,
                                TravelMeals as meals,
                                split(TravelDate,' ')[0] as travel_date,
                                split(BookingDate,' ')[0] as booking_date,
                                New_Revenue as revenue,
                                BookingCurrency as currency,
                                RecordLocator as record_locator,
                                IsCsp
                                )), 'map<string,string>'))) as Details
    FROM (
        SELECT PersonID,New_Revenue,TravelDate,BookingDate,TravelOrigin,TravelDestination,TravelDestination,TravelInsurance,TravelBaggage,TravelMeals,BookingCurrency,RecordLocator,IsCsp,
            ROW_NUMBER() OVER (PARTITION BY PersonID ORDER BY TravelDate DESC) AS row_num 
        FROM cdp_profiles
    ) AS subquery 
    WHERE row_num <= 250
    GROUP BY PersonID
    """
    )
    LOGGER.info("bookerccai checkpoint-3 -> Result1 count : "+str(result2.count()))
    checkpoint_str=result2.withColumn("strlen_details",F.length("Details")).select("PersonID","strlen_details").orderBy(F.desc("strlen_details")).limit(5).toPandas().to_csv(index=False)
    LOGGER.info("bookerccai checkpoint-4 : \n"+str(checkpoint_str))

    result = result1.join(result2, "PersonID")
    result.write.parquet(save_path_prefix + "/booker_details", mode="overwrite")


def passenger_compute(spark, df, save_path_prefix, LOGGER):
    lst = [
        "BookingMonth",
        "TravelMeals",
        "BookingChannel",
        "TravelBaggage",
        "TravelInsurance",
        "BookingCurrency",
        "IsRegistered",
        "TravelSoloOrGroup",
    ]
    # lst=['BookingMonth','TravelMeals','BookingChannel','IsEmployee','TravelBaggage','Gender','IsEmployeeDependent','TravelSoloOrGroup','IsRegistered','BookingCurrency']
    dfs = {}
    dfs1 = {}
    query = """SELECT passenger_hash,
                      MAX(FirstName) AS FirstName,
                      MAX(MiddleName) AS MiddleName,
                      MAX(LastName) AS LastName,
                      MAX(HonorificPrefix) AS HonorificPrefix,
                      MAX(HonorificSuffix) AS HonorificSuffix,
                      MAX(EmailAddress) AS EmailAddress,
                      MAX(Phone) AS Phone,
                      MAX(Gender) AS Gender,
                      MAX(ProvisionalPrimaryKey) AS ProvisionalPrimaryKey,
                      MAX(DateOfBirth) AS DateOfBirth,
                      MAX(Zipcode) AS Zipcode,
                      MAX(Nationality) AS Nationality,
                      MAX(City) AS City,
                      MAX(State) AS State,
                      Max(Bookingdate) AS LatestDate,
                      MIN(Bookingdate) AS FirstDate,
                      COUNT(PassengerID) AS TotalBookings,
                      SUM(New_Revenue) AS TotalRevenue"""
    for i in lst:
        dfs[i] = [row[0] for row in df.select(i).distinct().collect()]
        dfs1[i] = [
            word.replace(" ", "_space1_")
            .replace("-", "_space2_")
            .replace("(", "_space3_")
            .replace(")", "_space4_")
            for word in dfs[i]
        ]
        # print(dfs[i])
        for j, k in zip(dfs[i], dfs1[i]):
            query += (
                ",SUM(CASE WHEN "
                + i
                + ' = "'
                + j
                + '" THEN 1 ELSE 0 END) AS '
                + i
                + "_separator_"
                + k
                + ""
            )

    query += ",most_travelled(collect_list(TravelOrigin)) AS TravelOrigin,most_travelled(collect_list(TravelDestination)) AS TravelDestination,most_travelled(collect_list(TravelSeat)) AS TravelSeat"
    query += " FROM cdp_profiles GROUP BY passenger_hash"
    # print(query)
    df_dummy = spark.sql("select passenger_hash, count(*) as cnt from cdp_profiles group by passenger_hash")
    # df_dummy.write.mode("overwrite").parquet("s3a://cebu-cdp-data-dev/dedupe-cluster-1")

    LOGGER.info("ccai ui compute passenger first query started")
    result1 = spark.sql(query)
    # result1.write.mode("overwrite").parquet("s3a://cebu-cdp-data-dev/dedupe-cluster-2")

    LOGGER.info("ccai ui compute passenger first query completed" + str(result1.count()))
    # replaced from below code-> passenger_timeline(collect_list(Revenue), collect_list(TravelDate),collect_list(BookingDate),collect_list(TravelOrigin), collect_list(TravelDestination), collect_list(TravelInsurance), collect_list(TravelBaggage), collect_list(TravelMeals),collect_list(RecordLocator)) AS Details
    result2 = spark.sql(
        """
    SELECT passenger_hash, 
    passenger_timeline_new(collect_list(
            from_json(to_json(struct(TravelOrigin as origin,
                                TravelDestination as destination,
                                TravelInsurance as insurance,
                                TravelBaggage as baggage,
                                TravelMeals as meals,
                                split(TravelDate,' ')[0] as travel_date,
                                split(BookingDate,' ')[0] as booking_date,
                                Revenue as revenue,
                                RecordLocator as record_locator,
                                IsCsp
                                )), 'map<string,string>'))) as Details
    FROM (
        SELECT passenger_hash,Revenue,TravelDate,BookingDate,TravelOrigin,TravelDestination,TravelDestination,TravelInsurance,TravelBaggage,TravelMeals,RecordLocator,IsCsp,
            ROW_NUMBER() OVER (PARTITION BY passenger_hash ORDER BY TravelDate DESC) AS row_num 
        FROM cdp_profiles
    ) AS subquery 
    WHERE row_num <= 250
    GROUP BY passenger_hash
    """
    )
    # result2.write.mode("overwrite").parquet("s3a://cebu-cdp-data-dev/dedupe-cluster-3")

    LOGGER.info("ccai ui compute passenger second query completed " + str(result2.count()))

    result = result1.join(result2, "passenger_hash")
    LOGGER.info("ccai ui compute passenger last query completed " + str(result.count()))
    result.write.parquet(save_path_prefix + "/passenger_details", mode="overwrite")


def main_agg_compute(spark, df, save_path_prefix):
    result1 = spark.sql(
        "select count(distinct passenger_hash) as customers, count(distinct PassengerID) as journeys, count(distinct PersonID) as bookers, count(distinct BookingID) as bookings from cdp_profiles"
    )
    result1.show(truncate=False)
    result1.write.parquet(save_path_prefix + "/index_milestone", mode="overwrite")
    result2 = spark.sql(
        "select BookingCurrency, sum(Revenue) as Revenue from (select max(New_Revenue) as Revenue, max(BookingCurrency) as BookingCurrency from cdp_profiles group by BookingID) as currency group by BookingCurrency"
    )
    result2.show(truncate=False)
    result2.write.parquet(save_path_prefix + "/revenue_breakdown", mode="overwrite")


def compute_ui(config_path, spark, profile_path, LOGGER,day0_date):
    LOGGER.info("ccai Starting compute_ui")
    config = get_config(config_path)
    num_cores = spark.sparkContext.defaultParallelism

    end_date = profile_path.split("=")[1].strip('/')
    LOGGER.info(f"end date - {end_date}, day0_date - {day0_date}")
    spark.conf.set("spark.sql.shuffle.partitions", num_cores * 2)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.files.maxRecordsPerFile", 100000)
    spark.conf.set("spark.sql.files.maxPartitionBytes", 52428800)

    spark.udf.register("most_travelled", most_travelled)
    spark.udf.register("booker_timeline", booker_timeline)
    spark.udf.register("passenger_timeline", passenger_timeline)
    spark.udf.register("booker_timeline_new", booker_timeline_new)
    spark.udf.register("passenger_timeline_new", passenger_timeline_new)

    partition_paths = get_paths(day0_date,end_date,profile_path,LOGGER)

    LOGGER.info(f"ccai - ucp loaded partitions = {str(partition_paths)}")

    df = spark.read.parquet(*partition_paths).select(
        "ProvisionalPrimaryKey",
        "PersonID",
        "PassengerID",
        "BookingID",
        "BookerFirstName",
        "BookerLastName",
        "BookerEmailAddress",
        "BookerMobile",
        "Revenue",
        "BookerType",
        "BookerCity",
        "BookingDate",
        "TravelMeals",
        "BookingChannel",
        "IsEmployee",
        "TravelBaggage",
        "Gender",
        "IsEmployeeDependent",
        "TravelInsurance",
        "TravelSoloOrGroup",
        "IsRegistered",
        "BookingCurrency",
        "TravelOrigin",
        "TravelDestination",
        "FirstName",
        "MiddleName",
        "LastName",
        "HonorificPrefix",
        "HonorificSuffix",
        "EmailAddress",
        "Phone",
        "Zipcode",
        "DateOfBirth",
        "Nationality",
        "City",
        "State",
        "passenger_hash",
        "TravelSeat",
        "TravelDate",
        "RecordLocator",
        "IsCsp"
    )
    record_count = df.count()
    LOGGER.info("ccai -> Load Data : {} rows".format( record_count))
    df = df.dropDuplicates(subset = ['PassengerID'])
    record_count = df.count()
    LOGGER.info("ccai -> Load Data after dropping dups : {} rows".format( record_count))
    
    exchange_rates = {
        "THB": 0.657845,
        "AED": 13.203984,
        "MYR": 7.624044,
        "EUR": 0.011741,
        "OMR": 0.001127,
        "KWD": 0.003423,
        "VND": 254.181015,
        "USD": 0.019467,
        "AUD": 0.025128,
        "CNY": 0.126146,
        "PHP": 1.0,
        "KRW": 22.734294,
        "JPY": 2.180405,
        "HKD": 0.151516,
        "TWD": 0.539236,
        "QAR": 0.071024,
        "MOP": 0.156013,
        "IDR": 278.384131,
        "SAR": 0.073001,
        "INR": 1.427856,
        "BHD": 0.007356,
        "SGD": 0.026176,
        "BND": 0.026101,
    }
    for curr in exchange_rates:
        df = df.withColumn(
            curr,
            when(
                col("BookingCurrency") == curr,
                col("Revenue").cast("float") * exchange_rates[curr],
            ).otherwise(None),
        )

    df = df.withColumn("New_Revenue", greatest(*[col(curr) for curr in exchange_rates]))
    df = df.drop(*list(exchange_rates.keys()))
    df = df.withColumn(
        "Age",
        when(df.DateOfBirth == "Unknown", "Unknown").otherwise(
            expr("YEAR(CURRENT_DATE()) - YEAR(DateOfBirth)")
        ),
    )

    df = df.withColumn(
        "AgeRange",
        when(df.Age == "Unknown", "Unknown")
        .when((df.Age >= 1) & (df.Age <= 10), "1_to_10")
        .when((df.Age >= 11) & (df.Age <= 20), "11_to_20")
        .when((df.Age >= 21) & (df.Age <= 30), "21_to_30")
        .when((df.Age >= 31) & (df.Age <= 40), "31_to_40")
        .when((df.Age >= 41) & (df.Age <= 50), "41_to_50")
        .when((df.Age >= 51) & (df.Age <= 60), "51_to_60")
        .when((df.Age >= 61) & (df.Age <= 70), "61_to_70")
        .when((df.Age >= 71) & (df.Age <= 80), "71_to_80")
        .when((df.Age >= 81) & (df.Age <= 90), "81_to_90")
        .when((df.Age >= 91) & (df.Age <= 100), "91_to_100")
        .otherwise("Above100"),
    )

    df = df.withColumn("BookingMonth", date_format("BookingDate", "MMMM"))
    df.createOrReplaceTempView("cdp_profiles")
    LOGGER.info("ccai Starting ui compute main_agg_compute")
    main_agg_compute(spark, df, config["storageDetails"][3]["pathUrl"])
    LOGGER.info("ccai Starting ui compute passenger_compute")
    passenger_compute(spark, df, config["storageDetails"][3]["pathUrl"], LOGGER)
    LOGGER.info("ccai Starting ui compute booker_compute")
    booker_compute(spark, df, config["storageDetails"][3]["pathUrl"],LOGGER)
    LOGGER.info("ccai ui compute completed")
    return True