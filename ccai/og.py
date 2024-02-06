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
from pyspark.sql.functions import col,expr,when



def spark_conf(spark):
    spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    num_cores = spark.sparkContext.defaultParallelism

    spark.conf.set("spark.sql.shuffle.partitions", num_cores*2)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    # increase default parallelism
    # spark.conf.set("spark.default.parallelism", "200")


def get_config(file_path):
    # file_path = "s3a://cebu-cdp-data-qa/script/glue/cebu-cdp-profile-glue/profile_config_cebu_v1.json"
    bucket_name = file_path.split('/')[2]
    key = '/'.join(file_path.split('/')[3:])
    s3 = boto3.client('s3')
    config = json.loads(s3.get_object(
        Bucket=bucket_name, Key=key)['Body'].read())
    return config


def get_latest_partition(path, key, partition_date):
    # initiate s3 client
    s3 = boto3.client('s3')
    # get bucket name and prefix
    bucket_name = path.split('/')[2]
    prefix = '/'.join(path.split('/')[3:]) + '/'
    # bucket_name = 'edw-dl-process-data-dev'
    # prefix = 'data/odstest/hdata/odstest_all_diffen_open/odstest_all_booking/'
    # get all sub folfers names in the path
    response = s3.list_objects_v2(
        Bucket=bucket_name, Prefix=prefix, Delimiter='/')

    dates = []
    for page in response['CommonPrefixes']:
        if key in page['Prefix'].split('/')[-2]:
            # print(page['Prefix'].split('/')[-2])
            if partition_date in page['Prefix'].split('/')[-2]:
                return key + '=' + page['Prefix'].split('/')[-2].split('=')[1]
            dates.append(page['Prefix'].split('/')[-2])
    # sort dates in descending order
    dates.sort(reverse=True)
    return dates[0]


def filter_tables(config, source_dict, LOGGER):
    filter_arr = {
        "all_bookingcontact": "TypeCode = 'P' or TypeCode = 'W'",
        "all_passengertraveldoc": "DocTypeCode = 'OAFF' or DocTypeCode = 'S' or DocTypeCode = 'P' or DocTypeCode = 'CEB' or DocTypeCode = 'CEBD'",
        "all_personname": "NameType = 1",
        "all_personemail": "IsDefault = true",
        "all_traveldoc": "DocTypeCode = 'OAFF' or DocTypeCode = 'S' or DocTypeCode = 'P' or DocTypeCode = 'CEB' or DocTypeCode = 'GG' or DocTypeCode = 'CEBD'",
        "all_personaddress": "IsDefault = true",
        "all_personphone": "IsDefault = true AND (TypeCode = 'M' OR TypeCode = 'H')"
    }
    for each_source in config['entityDetails']:
        if each_source['entityName'] in filter_arr:
            source_dict[each_source['entityName']] = source_dict[each_source['entityName']].filter(
                filter_arr[each_source['entityName']])
            LOGGER.info("ccai -> Filter Tables - {} : {} rows".format(each_source['entityName'], source_dict[each_source['entityName']].count()))
            
    return source_dict


def get_columns_list(config):
    column_list = {}
    for eachMapping in config['attributeMapping']:
        for eachSourceMapping in eachMapping['sourceColumns']:
            if eachSourceMapping['entityName'] in column_list:
                column_list[eachSourceMapping['entityName']].append(
                    eachSourceMapping['columnName'])
            else:
                column_list[eachSourceMapping['entityName']] = [
                    eachSourceMapping['columnName']]
    for eachMapping in config['columnMapping']:
        column_list[eachMapping['sourceEntity']].append(
            eachMapping['sourceColumn'])
    # apply distinct on each list
    for eachColumnList in column_list:
        column_list[eachColumnList] = list(set(column_list[eachColumnList]))
    # column_list["all_booking"].append("RecordLocator")
    # column_list["all_booking"].append("Status")
    # column_list["all_payment"].append("AuthorizationStatus")
    return column_list

def list_folders_in_path(bucket_name, prefix,LOGGER,start_date,end_date):
    s3 = boto3.client('s3')
    available_folders = []

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')



    # for common_prefix in response.get('CommonPrefixes', []):
    #     folder_name = common_prefix.get('Prefix', '').rstrip('/')
    #     available_folders.append(folder_name)

    # # LOGGER.info(f"Available folders: {available_folders}")

    # available_partitions = []

    # for partition in available_folders:
    #         if available_folders:
    #             split_partition = partition.split("=")
    #             available_date = split_partition[1]
    #             if start_date <= available_date <= end_date:
    #                 available_partitions.append(available_date)
    
    available_folders = [common_prefix.get('Prefix', '').rstrip('/') for common_prefix in response.get('CommonPrefixes', [])]

    available_partitions = [partition.split("=")[1] for partition in available_folders if available_folders]

    available_partitions = [date for date in available_partitions if start_date <= date <= end_date]

    return available_partitions


# def getpaths(available_partitions,entityName,path_url):
#     path = "/".join(path_url.split("/")[:7]) + "/" + path_url.split("/")[4] + "_" + entityName + "/nds="
#     partition_paths = []
            
#     for date in available_partitions:
#         available_path = path + date + "/"
#         partition_paths.append(available_path)

#     return partition_paths

def getpaths(available_partitions, entityName, path_url):
    base_path = "/".join(path_url.split("/")[:7]) + "/" + path_url.split("/")[4] + "_" + entityName + "/nds="
    
    return [base_path + date + "/" for date in available_partitions]


def get_bucket(config,entityName):

    """
    Returns like this
    bucket - edw-dl-process-data-dev
    prefix - data/odstest/hdata/odstest_all_diffen_nonopen/odstest_all_bookingpassenger/
    
    """

    path = config["entityDetails"][0]["pathUrl"]
    bucket = path.split("/")[2]
    prefix = '/'.join(path.split("/")[3:-1])+"/"+ path.split("/")[4] + "_" + entityName + "/"
    return bucket,prefix

# def load_data(config, partition_date, spark, LOGGER):
#     column_list = get_columns_list(config)
#     source_dict = {}
#     for each_source in config['entityDetails']:
#         if each_source['entityType'] == 'snapshot':
#             # get latest partition
#             latest_partition = get_latest_partition(
#                 each_source['pathUrl'], each_source['partitionDetails']['partitionColumn'][0], partition_date)
#             required_partition = "{{{},2023-07-31}}".format(latest_partition) #"ods={2023-03-21,}"
#             # read orc data using spark
#             source_dict[each_source['entityName']] = spark.read.load(
#                 each_source['pathUrl'] + "/" + latest_partition, format=each_source['dataFormat']).select(column_list[each_source['entityName']])
#             record_count = source_dict[each_source['entityName']].count()
#             if record_count==0:
#                 LOGGER.info("ccai -> Record count 0 for " + str(each_source['entityName']))
#                 source_dict[each_source['entityName']] = spark.read.load(
#                     each_source['pathUrl'] + "/" + "ods=2023-07-31", format=each_source['dataFormat']).select(column_list[each_source['entityName']])
#                 record_count = source_dict[each_source['entityName']].count()
#             LOGGER.info("ccai -> Load Data - {} : {} rows".format(each_source['entityName'], record_count))
#     source_dict = filter_tables(config, source_dict, LOGGER)
#     all_fee_debug = source_dict['all_fee'].limit(10).toPandas().to_csv(index=False)
#     LOGGER.info("ccai -> all fee data\n\n"+all_fee_debug)
#     # import sys
#     # sys.exit("ccai Exit -> all fee data")
#     return source_dict



def load_data(config,spark,LOGGER,partition_date,archive_date):

    """
    Archive , non open and a business date is been loaded 
    Date's -
                Archive data date = 2021-01-01
                Business Date(ex) = 2023-07-31
                Non Open = From 2021-01-01 to 2023-07-31

    """

    column_list = get_columns_list(config)
    source_dict = {}
    for each_source in config['entityDetails']:
        if each_source['entityType'] == 'snapshot':
            
            # load open partitions - archive and a business date
            open_paths = [
                each_source['pathUrl'].replace("nonopen", "open") + "/ods=" + date + "/"
                for date in [partition_date, archive_date]
            ]

            try:
                #try loading both partitions
                source_dict[each_source['entityName']] = spark.read.load(
                    open_paths, format=each_source['dataFormat']).select(column_list[each_source['entityName']])
                record_count = source_dict[each_source['entityName']].count()
                LOGGER.info("ccai -> Load Data b&a - {} : {} rows".format(each_source['entityName'], record_count))
            except:
                #try loading business date partition
                source_dict[each_source['entityName']] = spark.read.load(
                    open_paths[0], format=each_source['dataFormat']).select(column_list[each_source['entityName']])
                record_count = source_dict[each_source['entityName']].count()
                LOGGER.info("ccai -> Load Data b - {} : {} rows".format(each_source['entityName'], record_count))
                
        
            #get available non open partitions
            path = each_source['pathUrl']
            resource_name = each_source['entityName']

            if each_source['entityName'] == "all_booking" or each_source['entityName'] == "all_passengerjourneysegment":
                path = each_source['pathUrl'] + "version"  
                resource_name = each_source['entityName'] + "version"

            bucket,prefix = get_bucket(config,resource_name)
            available_partitions = list_folders_in_path(bucket, prefix,LOGGER,archive_date,partition_date)
            partition_paths = getpaths(available_partitions,resource_name,path)
            

            #load only if partitions are available
            if partition_paths:
                ##should edit
                non_open_df = spark.read.load(
                        partition_paths, format=each_source['dataFormat']).where('c_operationtype = "D"').select(column_list[each_source['entityName']])

                if each_source['entityName'] == "all_passengerjourneysegment":
                    non_open_df = spark.read.load(
                        partition_paths, format=each_source['dataFormat']).where('c_operationtype = "D"').where("VersionEndUTC == '9999-12-31 00:00:00.000'").select(column_list[each_source['entityName']])

                
                source_dict[each_source['entityName']] = source_dict[each_source['entityName']].unionAll(non_open_df)
                record_count = non_open_df.count()
                LOGGER.info("ccai -> Load Data non open  - {} : {} rows".format(resource_name, record_count))

            record_count = source_dict[each_source['entityName']].count()
            LOGGER.info("ccai -> Load Data - {} : {} rows".format(each_source['entityName'], record_count))
            LOGGER.info(f"{each_source['entityName']} loaded successfully")

            ##<------- open 2023-07-31 -------->##
            # source_dict[each_source['entityName']] = spark.read.load(
            #         each_source['pathUrl'] + "/" + "ods=2023-07-31", format=each_source['dataFormat']).select(column_list[each_source['entityName']])
            # record_count = source_dict[each_source['entityName']].count()
            # LOGGER.info("ccai -> Load Data - {} : {} rows".format(each_source['entityName'], record_count))

    source_dict = filter_tables(config, source_dict,LOGGER)

    return source_dict 


def derived_tables(config, source_dict, spark, LOGGER):
    derive_dict = {
        "all_passengerjourneyleg": ["select * from all_passengerjourneyleg order by PassengerID, ModifiedUTC desc", "SELECT PassengerID, CONCAT_WS('_',COLLECT_LIST(UnitDesignator)) AS UnitDesignator FROM all_passengerjourneyleg GROUP BY PassengerID"],
        "GetTravelDestination_all_passengerjourneysegment": ["SELECT * from all_passengerjourneysegment order by PassengerID, JourneyNumber", "select PassengerID, CONCAT_WS('_',COLLECT_LIST(ArrivalStation)) as ArrivalStation FROM GetTravelDestination_all_passengerjourneysegment GROUP BY PassengerID"],
        # "GetTravelDestination_all_passengerjourneysegment": ["SELECT all_passengerjourneysegment.PassengerID AS PassengerID, all_passengerjourneysegment.ArrivalStation AS ArrivalStation FROM ( SELECT PassengerID, max(JourneyNumber) AS maxJourneyNumber FROM all_passengerjourneysegment GROUP BY PassengerID ) AS all_passengerjourneysegment_max JOIN all_passengerjourneysegment ON all_passengerjourneysegment_max.PassengerID = all_passengerjourneysegment.PassengerID AND all_passengerjourneysegment_max.maxJourneyNumber = all_passengerjourneysegment.JourneyNumber"]
        "GetPassportNumber_all_passengertraveldoc": ["SELECT all_passengertraveldoc.PassengerID AS PassengerID, all_passengertraveldoc.DocNumber AS DocNumber, all_passengertraveldoc.IssuedByCode AS IssuedByCode, all_passengertraveldoc.ExpirationDate AS ExpirationDate FROM all_passengertraveldoc WHERE upper(all_passengertraveldoc.DocTypeCode) = 'P' AND (all_passengertraveldoc.PassengerID, all_passengertraveldoc.ExpirationDate) IN ( SELECT all_passengertraveldoc.PassengerID AS PassengerID, max(all_passengertraveldoc.ExpirationDate) AS maxExpirationDate FROM all_passengertraveldoc where upper(all_passengertraveldoc.DocTypeCode) = 'P' GROUP BY all_passengertraveldoc.PassengerID )"],
        "GetPassportNumber_all_traveldoc": ["SELECT all_traveldoc.PersonID AS PersonID, all_traveldoc.DocNumber AS DocNumber, all_traveldoc.IssuedByCode AS IssuedByCode, all_traveldoc.ExpirationDate AS ExpirationDate FROM all_traveldoc WHERE upper(all_traveldoc.DocTypeCode) = 'P' AND (all_traveldoc.PersonID, all_traveldoc.ExpirationDate) IN ( SELECT all_traveldoc.PersonID AS PersonID, max(all_traveldoc.ExpirationDate) AS maxExpirationDate FROM all_traveldoc where upper(all_traveldoc.DocTypeCode) = 'P' GROUP BY all_traveldoc.PersonID)"],
        "all_fee": ["select all_passengerfee.PassengerID,all_passengerfee.FeeCode, all_fee.Name from all_passengerfee left join all_fee on all_passengerfee.FeeCode = all_fee.FeeCode"],
        "all_passengerjourneysegment": ["select * from all_passengerjourneysegment where JourneyNumber = 1"],
        "GetTravelSoloOrGroup_all_bookingpassenger": ["select BookingID, case when count(distinct PassengerID) > 1 then 'Group' else 'Solo' end as TravelSoloOrGroup from all_bookingpassenger group by BookingID"]
    }
    mask_sources = ["all_passengerfee"]
    # create temp tables for all sources
    for each_source in source_dict:
        source_dict[each_source].createOrReplaceTempView(each_source)
    # create derived tables
    for each_derived in derive_dict:
        for each_tran in derive_dict[each_derived]:
            source_dict[each_derived] = spark.sql(each_tran)
            source_dict[each_derived].createOrReplaceTempView(each_derived)
        LOGGER.info("ccai -> Derived Table - {} : {} rows".format(each_derived, source_dict[each_derived].count()))
        
    # mask/remove tables
    for each_mask in mask_sources:
        source_dict.pop(each_mask)
    return source_dict


def transaction_conversion(config, source_dict, spark,LOGGER):
    transaction_dict = {
        "all_bookingcontact": {
            "key": "BookingID",
            "mapColumn": "TypeCode"
        },
        "all_passengertraveldoc": {
            "key": "PassengerID",
            "mapColumn": "DocTypeCode"
        },
        "all_fee": {
            "key": "PassengerID",
            "mapColumn": "FeeCode"
        },
        "all_traveldoc": {
            "key": "PersonID",
            "mapColumn": "DocTypeCode"
        },
        "all_personphone": {
            "key": "PersonID",
            "mapColumn": "TypeCode"
        }
    }
    all_fee_debug = source_dict['all_fee'].limit(10).toPandas().to_csv(
        index=False)
    LOGGER.info("ccai -> Transaction Conversion start : \n" + all_fee_debug)

    column_list = get_columns_list(config)
    for each_source in transaction_dict:
        source_dict[each_source].createOrReplaceTempView(each_source)
        query = "select " + transaction_dict[each_source]['key']
        for each_column in column_list[each_source]:
            if each_column != transaction_dict[each_source]['key'] and each_column != transaction_dict[each_source]['mapColumn']:
                query += ", struct(" + transaction_dict[each_source]['mapColumn'] + \
                    ", " + each_column + ") as " + each_column
        query += " from " + each_source
        if each_source=='all_fee':
            query += "  where {} is not null".format(transaction_dict['all_fee']['mapColumn'])
        source_dict[each_source] = spark.sql(query)
        source_dict[each_source].createOrReplaceTempView(each_source)

        if each_source=='all_fee':
            all_fee_debug = source_dict['all_fee'].limit(10).toPandas().to_csv(
            index=False)
            LOGGER.info("ccai -> Transaction Conversion mid1 : \n" + all_fee_debug)
        # source_dict[each_source] = source_dict[each_source].fillna(0, subset=[list(source_dict[each_source].columns).remove(transaction_dict[each_source]['key'])])
        # building query select col1, collect_list(col2) as k, collect_list(col3) as v from df_transactional group by col1
        query = "select " + transaction_dict[each_source]['key']
        for each_column in column_list[each_source]:
            if each_column != transaction_dict[each_source]['key'] and each_column != transaction_dict[each_source]['mapColumn']:
                query += ", collect_list(" + each_column + \
                    ") as " + each_column
        query += " from " + each_source + " group by " + \
            transaction_dict[each_source]['key']
        source_dict[each_source] = spark.sql(query)
        source_dict[each_source].createOrReplaceTempView(each_source)
        if each_source=='all_fee':
            all_fee_debug = source_dict['all_fee'].limit(10).toPandas().to_csv(
                index=False)
            LOGGER.info("ccai -> Transaction Conversion mid2 : \n" + all_fee_debug)
        # building query select col1, map(col2, col3) as k, map(col4, col5) as v from df_transactional
        query = "select " + transaction_dict[each_source]['key']
        for each_column in column_list[each_source]:
            if each_column != transaction_dict[each_source]['key'] and each_column != transaction_dict[each_source]['mapColumn']:
                query += ", map_from_entries(" + \
                    each_column + ") as " + each_column
        query += " from " + each_source

        source_dict[each_source] = spark.sql(query)
        source_dict[each_source].createOrReplaceTempView(each_source)

        LOGGER.info("ccai -> Transaction Conversation - {} : {} rows".format(each_source, source_dict[each_source].count()))
    for each_source in source_dict:
        LOGGER.info("ccai -> Transaction Conversation Src Dict - {} : {} rows".format(each_source, source_dict[each_source].count()))
    all_fee_debug = source_dict['all_fee'].limit(10).toPandas().to_csv(
        index=False)
    LOGGER.info("ccai -> Transaction Conversion end : \n" + all_fee_debug)
    LOGGER.info("ccai -> Transaction Conversion end count : " + str(source_dict['all_fee'].filter("PassengerID is null").count()))
    LOGGER.info("ccai -> Transaction Conversion end non null : " + str(source_dict['all_fee'].filter("PassengerID is null").withColumn("Name",F.expr("cast (Name as string)")).toPandas().to_csv(index=False)))
    return source_dict, transaction_dict


def join_entities(config, source_dict, spark, LOGGER):
    join_dict = {
        "all_bookingpassenger": {
            "all_booking": "BookingID",
            "all_bookingcontact": "BookingID",
            "GetTravelSoloOrGroup_all_bookingpassenger": "BookingID",
            "all_passengeraddress": "PassengerID",
            "all_passengertraveldoc": "PassengerID",
            "all_passengerjourneysegment": "PassengerID",
            "all_passengerjourneyleg": "PassengerID",
            "GetTravelDestination_all_passengerjourneysegment": "PassengerID",
            "GetPassportNumber_all_passengertraveldoc": "PassengerID",
            "all_fee": "PassengerID"
        },
        "all_person": {
            "all_personname": "PersonID",
            "all_personemail": "PersonID",
            "all_personphone": "PersonID",
            "all_personaddress": "PersonID",
            "all_traveldoc": "PersonID",
            "GetPassportNumber_all_traveldoc": "PersonID", #"all_agent": "PersonID"
        }
    }
    # iterate over source_dict
    for each_source in source_dict:
        source_dict[each_source] = source_dict[each_source].select([F.col(each_column).alias(
            each_source + "_" + each_column) for each_column in source_dict[each_source].columns])
        source_dict[each_source].createOrReplaceTempView(each_source)
        LOGGER.info("ccai -> join entities - {} : {}".format(each_source, source_dict[each_source].count()))

    all_fee_debug = source_dict['all_fee'].filter("all_fee_PassengerID is null").limit(10).withColumn("all_fee_Name",F.expr("cast(all_fee_Name as string)")).toPandas().to_csv(index=False)
    LOGGER.info("ccai -> join entities all fee non nulls : " + str(source_dict['all_fee'].filter("all_fee_PassengerID is null").count()))
    
    # import sys
    # sys.exit("ccai -> all fee debug")
    # join all_booking and all_agent on CreatedUserID and AgentID
    # joined_df = spark.sql("select * from all_booking")

    joined_df = spark.sql(
        "select * from all_booking full outer join all_agent on all_booking.all_booking_CreatedUserID <=> all_agent.all_agent_AgentID")
    joined_df.cache()
    joined_df.createOrReplaceTempView("joined_df")
    LOGGER.info("ccai -> Type of joined_df : {}".format( type(joined_df)))
    LOGGER.info("ccai -> join entities - check1 : {}".format( joined_df.count()))
    joined_df.createOrReplaceTempView("joined_df")

    # join all_bookingpassenger on BookingID

    joined_df = spark.sql(
        "select * from joined_df left join all_bookingpassenger on joined_df.all_booking_BookingID <=> all_bookingpassenger.all_bookingpassenger_BookingID")
    joined_df.createOrReplaceTempView("joined_df")
    LOGGER.info("ccai -> join entities - check2 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check2 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))

    distinct_booking = spark.sql(
            "SELECT COUNT(DISTINCT all_bookingpassenger_BookingID) AS distinct_count FROM joined_df"
        )
    result = distinct_booking.first()
    LOGGER.info("ccai -> all_bookingpassenger distinct booking count after join : {}".format(result.distinct_count))

    # join all_person on PersonID
    joined_df = spark.sql(
        "select * from joined_df left join all_person on joined_df.all_agent_PersonID <=> all_person.all_person_PersonID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check3 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check3 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))

    # join all_personname on PersonID
    joined_df = spark.sql(
        "select * from joined_df left join all_personname on joined_df.all_person_PersonID <=> all_personname.all_personname_PersonID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check4 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check4 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))


    # join all_personemail on PersonID
    joined_df = spark.sql(
        "select * from joined_df left join all_personemail on joined_df.all_person_PersonID <=> all_personemail.all_personemail_PersonID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check5 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check5 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))

    # join all_personphone on PersonID
    joined_df = spark.sql(
        "select * from joined_df left join all_personphone on joined_df.all_person_PersonID <=> all_personphone.all_personphone_PersonID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check6 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check6 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))

    # join all_personaddress on PersonID
    joined_df = spark.sql(
        "select * from joined_df left join all_personaddress on joined_df.all_person_PersonID <=> all_personaddress.all_personaddress_PersonID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check7 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check7 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))

    # join all_traveldoc on PersonID
    joined_df = spark.sql(
        "select * from joined_df left join all_traveldoc on joined_df.all_person_PersonID <=> all_traveldoc.all_traveldoc_PersonID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check8 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check8 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))

    # join GetPassportNumber_all_traveldoc on PersonID
    joined_df = spark.sql(
        "select * from joined_df left join GetPassportNumber_all_traveldoc on joined_df.all_person_PersonID <=> GetPassportNumber_all_traveldoc.GetPassportNumber_all_traveldoc_PersonID")
    joined_df.createOrReplaceTempView("joined_df")
    joined_df.cache()
    # LOGGER.info("ccai -> join entities - check9 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check9 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))

    joined_df = spark.sql(
        "select * from joined_df left join all_payment on joined_df.all_booking_BookingID <=> all_payment.all_payment_BookingID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check10 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check10 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))
    # join all_bookingcontact on BookingID
    joined_df = spark.sql(
        "select * from joined_df left join all_bookingcontact on joined_df.all_booking_BookingID <=> all_bookingcontact.all_bookingcontact_BookingID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check11 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check11 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))

    # join GetTravelSoloOrGroup_all_bookingpassenger on BookingID
    joined_df = spark.sql("select * from joined_df left join GetTravelSoloOrGroup_all_bookingpassenger on joined_df.all_booking_BookingID <=> GetTravelSoloOrGroup_all_bookingpassenger.GetTravelSoloOrGroup_all_bookingpassenger_BookingID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check12 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check12 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))

    # join all_passengeraddress on PassengerID
    joined_df = spark.sql(
        "select * from joined_df left join all_passengeraddress on joined_df.all_bookingpassenger_PassengerID <=> all_passengeraddress.all_passengeraddress_PassengerID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check13 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check13 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))

    # join all_passengertraveldoc on PassengerID
    joined_df = spark.sql(
        "select * from joined_df left join all_passengertraveldoc on joined_df.all_bookingpassenger_PassengerID <=> all_passengertraveldoc.all_passengertraveldoc_PassengerID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check14 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check14 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))

    # join GetPassportNumber_all_passengertraveldoc on PassengerID
    joined_df = spark.sql("select * from joined_df left join GetPassportNumber_all_passengertraveldoc on joined_df.all_bookingpassenger_PassengerID <=> GetPassportNumber_all_passengertraveldoc.GetPassportNumber_all_passengertraveldoc_PassengerID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check15 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check15 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))

    # join all_passengerjourneysegment on PassengerID
    joined_df = spark.sql(
        "select * from joined_df left join all_passengerjourneysegment on joined_df.all_bookingpassenger_PassengerID <=> all_passengerjourneysegment.all_passengerjourneysegment_PassengerID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check16 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check16 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))


    # join GetTravelDestination_all_passengerjourneysegment on PassengerID
    joined_df = spark.sql("select * from joined_df left join GetTravelDestination_all_passengerjourneysegment on joined_df.all_bookingpassenger_PassengerID <=> GetTravelDestination_all_passengerjourneysegment.GetTravelDestination_all_passengerjourneysegment_PassengerID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check17 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check17 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))


    # join all_passengerjourneyleg on PassengerID
    joined_df = spark.sql(
        "select * from joined_df left join all_passengerjourneyleg on joined_df.all_bookingpassenger_PassengerID <=> all_passengerjourneyleg.all_passengerjourneyleg_PassengerID")
    joined_df.createOrReplaceTempView("joined_df")
    # LOGGER.info("ccai -> join entities - check18 : {}".format( joined_df.count()))
    # LOGGER.info("ccai -> bookingid count - check18 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))

    # join all_fee on PassengerID
    joined_df = spark.sql(
        "select * from joined_df left join all_fee on joined_df.all_bookingpassenger_PassengerID <=> all_fee.all_fee_PassengerID")
    joined_df.cache()
    joined_df.createOrReplaceTempView("joined_df")
    LOGGER.info("ccai -> join entities - check19 : {}".format( joined_df.count()))
    LOGGER.info("ccai -> bookingid count - check19 : {}".format(joined_df.select('all_bookingpassenger_BookingID').distinct().count()))


    return joined_df
    # join_dict = {
    #     "all_bookingpassenger": {
    #         "all_booking": "BookingID",
    #         "all_bookingcontact": "BookingID",
    #         "GetTravelSoloOrGroup_all_bookingpassenger": "BookingID",
    #         "all_passengeraddress": "PassengerID",
    #         "all_passengertraveldoc": "PassengerID",
    #         "all_passengerjourneysegment": "PassengerID",
    #         "all_passengerjourneyleg": "PassengerID",
    #         "GetTravelDestination_all_passengerjourneysegment": "PassengerID",
    #         "GetPassportNumber_all_passengertraveldoc": "PassengerID",
    #         "all_fee": "PassengerID"
    #     },
    #     "all_person": {
    #         "all_personname": "PersonID",
    #         "all_personemail": "PersonID",
    #         "all_personphone": "PersonID",
    #         "all_personaddress": "PersonID",
    #         "all_traveldoc": "PersonID",
    #         "GetPassportNumber_all_traveldoc": "PersonID",
    #         "all_agent": "PersonID"
    #     }
    # }
    # master_tables = {}
    # for each_master in join_dict:
    #     master_tables[each_master] = source_dict[each_master].select([F.col(each_column).alias(
    #         each_master + "_" + each_column) for each_column in source_dict[each_master].columns])
    #     for each_source in join_dict[each_master]:
    #         source_dict[each_source] = source_dict[each_source].select([F.col(each_column).alias(
    #             each_source + "_" + each_column) for each_column in source_dict[each_source].columns])
    #         # join master table with each source on join_dict[each_master][each_source]
    #         master_tables[each_master] = master_tables[each_master].join(
    #             source_dict[each_source], master_tables[each_master][each_master + "_" + join_dict[each_master][each_source]] == source_dict[each_source][each_source + "_" + join_dict[each_master][each_source]], "left")
    # return master_tables


def create_profile(config):
    profileSchema = []
    column_list = []
    profileSchema_dict = {}
    for each in config["profileSchema"]:
        column_list.append(each['columnName'])
        if each['dataType'] == 'string':
            profileSchema.append(StructField(
                each['columnName'], StringType(), True))
            profileSchema_dict[each['columnName']] = StringType()
        elif each['dataType'] == 'integer':
            profileSchema.append(StructField(
                each['columnName'], IntegerType(), True))
            profileSchema_dict[each['columnName']] = IntegerType()
        elif each['dataType'] == 'long':
            profileSchema.append(StructField(
                each['columnName'], LongType(), True))
            profileSchema_dict[each['columnName']] = LongType()
        elif each['dataType'] == 'double':
            profileSchema.append(StructField(
                each['columnName'], DoubleType(), True))
            profileSchema_dict[each['columnName']] = DoubleType()
        elif each['dataType'] == 'boolean':
            profileSchema.append(StructField(
                each['columnName'], BooleanType(), True))
            profileSchema_dict[each['columnName']] = BooleanType()
        elif each['dataType'] == 'date':
            profileSchema.append(StructField(
                each['columnName'], DateType(), True))
            profileSchema_dict[each['columnName']] = DateType()

    profileSchema = StructType(profileSchema)
    # profile_df = spark.createDataFrame([], profileSchema)
    # return profile_df, column_list
    return column_list, profileSchema_dict


def skew_outer_join(left_df, right_df, left_join_col, right_join_col, spark, LOGGER):
    # cast left_join_col in left_df to int
    left_df = left_df.withColumn(
        left_join_col, F.col(left_join_col).cast(IntegerType()))
    # cast right_join_col in right_df to int
    right_df = right_df.withColumn(
        right_join_col, F.col(right_join_col).cast(IntegerType()))

    def get_skewed_values(df, column_name="all_booking_CreatedUserID"):
        df.createOrReplaceTempView("skewed_dataset")
        query = "select " + column_name + \
            ", count(" + column_name + ") as count from skewed_dataset group by " + \
            column_name + " order by count desc"
        df = spark.sql(query)
        # df.repartition(1).write.mode("overwrite").parquet("s3a://cebu-cdp-data-qa/bookingpassmaster")
        df = df.pandas_api()
        # sort bookingpassmaster and personmaster by count
        df = df.sort_values(by=['count'], ascending=False)
        # get top 50 all_booking_CreatedUserID as a list
        return df[column_name].head(50).to_numpy(), df['count'].head(50).to_numpy(), df[column_name].to_numpy()
    left_skewed_list, left_skewed_count, left_full_list = get_skewed_values(
        left_df, left_join_col)
    right_skewed_list, right_skewed_count, right_full_list = get_skewed_values(
        right_df, right_join_col)
    LOGGER.info("CCAI get_skewed_values done")
    all_value_tuple = []
    for each in list(set([*left_full_list, *right_full_list])):
        # skip NaN
        if not ps.isna(each):
            all_value_tuple.append(int(each))
    print(all_value_tuple)
    # print type of left_join_col in left_df
    # print(left_df.select(left_join_col).dtypes[0][1])
    all_value_df = spark.createDataFrame(all_value_tuple, left_df.select(
        left_join_col).dtypes[0][1]).withColumnRenamed("value", "all_join_vals")
    LOGGER.info("CCAI all_value_df count is " + str(all_value_df.count()))
    # broadcast left join all_value_df with left_df with hint in sql
    all_value_df.createOrReplaceTempView("all_value_df")
    left_df.createOrReplaceTempView("left_df")
    right_df.createOrReplaceTempView("right_df")
    # broadcast left join all_value_df with left_df
    left_df = spark.sql(
        "select /*+ BROADCAST(all_value_df) */ all_value_df.all_join_vals, left_df.* from all_value_df left join left_df on all_value_df.all_join_vals = left_df." + left_join_col)
    LOGGER.info("CCAI left_df count after broadcast join"+str(left_df.count()))
    # broadcast left join all_value_df with right_df with hint
    right_df = spark.sql(
        "select /*+ BROADCAST(all_value_df) */ all_value_df.all_join_vals, right_df.* from all_value_df left join right_df on all_value_df.all_join_vals = right_df." + right_join_col)
    LOGGER.info("CCAI right_df count after broadcast join" +
                str(right_df.count()))
    left_df = left_df.drop(left_join_col).withColumnRenamed(
        "all_join_vals", left_join_col)
    right_df = right_df.drop(right_join_col).withColumnRenamed(
        "all_join_vals", right_join_col)
    # left_df.show()
    # right_df.show()

    def left_skew_handler(x, skew_list, skew_count):
        if int(x) in skew_list:
            # get index of x in skew_list
            iter_range = int(
                skew_count[np.where(skew_list == int(x))[0][0]]/1000)
            return str(int(x)) + "_" + str(random.randint(0, iter_range))
        else:
            return x
    skew_handler_udf = F.udf(lambda x: left_skew_handler(
        x, left_skewed_list, left_skewed_count), StringType())
    left_df = left_df.withColumn(
        left_join_col+"_salted", skew_handler_udf(F.col(left_join_col)))
    LOGGER.info("CCAI left_df count after left skew handler" +
                str(left_df.count()))

    def prep_right_df(all_list, skew_list, skew_count):
        right_values = []
        print(skew_list)
        for each in all_list:
            if int(each) in skew_list:
                # print(each)
                # get index of x in skew_list
                iter_range = int(
                    skew_count[np.where(skew_list == int(each))[0][0]]/1000)
                for i in range(iter_range+1):
                    right_values.append((int(each), str(int(each))+"_"+str(i)))
            else:
                right_values.append((int(each), str(int(each))))

        return right_values
    right_values = prep_right_df(
        all_value_tuple, left_skewed_list, left_skewed_count)
    right_df_join = spark.createDataFrame(
        right_values, [right_join_col, right_join_col+"_salted"])
    LOGGER.info("CCAI right_df_join count prep_right_df " +
                str(right_df_join.count()))
    right_df_join.createOrReplaceTempView("right_df_join")
    right_df.createOrReplaceTempView("right_df")
    right_df = spark.sql("select /*+ BROADCAST(right_df_join) */ right_df_join."+right_join_col +
                         "_salted, right_df.* from right_df_join left join right_df on right_df_join." + right_join_col+" = right_df." + right_join_col)
    # right_df = right_df.drop(right_df[right_join_col])
    LOGGER.info("CCAI right_df count after right skew handler" +
                str(right_df.count()))

    # inner join left_df and right_df
    left_df.createOrReplaceTempView("left_df")
    right_df.createOrReplaceTempView("right_df")
    # left_df.show()
    # right_df.show()
    joined_df = spark.sql("select left_df.*, right_df.* from left_df inner join right_df on left_df." +
                          left_join_col+"_salted = right_df." + right_join_col+"_salted")
    LOGGER.info("CCAI joined_df count after inner join"+str(joined_df.count()))
    # joined_df.write.mode("overwrite").parquet("s3a://cebu-cdp-data-qa/joined")
    # joined_df.show()
    return joined_df


def master_profile_merge(config, profile_df, master_tables, source_dict, spark):
    # for each_master in master_tables:
    # master_tables[each_master].createOrReplaceTempView(each_master)
    # left join master_tables["all_bookingpassenger"] with profile_df
    profile_df.createOrReplaceTempView("profiles")
    master_tables["all_bookingpassenger"].createOrReplaceTempView(
        "all_bookingpassengermaster")
    master_tables["all_person"].createOrReplaceTempView("all_personmaster")
    profile_df = spark.sql(
        "select * from all_bookingpassengermaster left join profiles on all_bookingpassengermaster.all_bookingpassenger_PassengerID = profiles.PassengerID")
    profile_df = skew_outer_join(
        profile_df, master_tables["all_person"], "all_booking_CreatedUserID", "all_agent_AgentID")
    # profile_df.createOrReplaceTempView("profiles")
    # outer join profiles with all_personmaster on all_personmaster.all_agent_AgentID = profiles.all_booking_CreatedUserID
    # profile_df = spark.sql(
    # "select * from profiles full outer join all_personmaster on all_personmaster.all_agent_AgentID = profiles.all_booking_CreatedUserID")

    profile_df.createOrReplaceTempView("profiles")
    return profile_df

def transformationsNew(config, profile_df, transaction_dict, spark,LOGGER,partition_date,source_dict_copy):
    LOGGER.info("ccai - transformations - start")
    profile_df.createOrReplaceTempView("profiles")
    new_column_queries = list()
    for each_column in config["columnMapping"]:
        # each_column: {'sourceEntity': 'all_bookingpassenger', 'sourceColumn': 'PassengerID', 'targetColumn': 'PassengerID'}
        #profile_df = spark.sql("select *, " + each_column['sourceEntity'] + "_" +
        #                       each_column['sourceColumn'] + " as " + each_column['targetColumn'] + " from profiles")
        query = each_column['sourceEntity'] + "_" + each_column['sourceColumn'] + " as " + each_column['targetColumn']
        new_column_queries.append(query)
    for each_transformation in config["attributeMapping"]:
        #profile_df.createOrReplaceTempView("profiles")
        if each_transformation['transformationFunction'] == 'GetTravelSoloOrGroup':
            #profile_df = spark.sql("select *,GetTravelSoloOrGroup_all_bookingpassenger_TravelSoloOrGroup  as " +
            #                       each_transformation['attributeName'] + " from profiles")
            query = "GetTravelSoloOrGroup_all_bookingpassenger_TravelSoloOrGroup  as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetPersonID':
            # profile_df = spark.sql("select *,all_person_PersonID as " +
            #                        each_transformation['attributeName'] + " from profiles")
            query = "all_person_PersonID as " +  each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetProvisionalPrimaryKey':
            #profile_df = spark.sql("select *,concat(all_bookingpassenger_PassengerID,'_',all_person_PersonID) as " + each_transformation['attributeName'] + " from profiles")
            query = "concat(all_bookingpassenger_PassengerID,'_',all_person_PersonID) as " + each_transformation['attributeName'] 
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetIsRegistered':
            # profile_df = spark.sql("""
            #     select *, 
            #     case when """+each_transformation['sourceColumns'][0]['entityName']+"""_"""+each_transformation['sourceColumns'][0]['columnName']+""" is null and """+each_transformation['sourceColumns'][1]['entityName']+"""_"""+each_transformation['sourceColumns'][1]['columnName']+""" is null then
            #         case when """+each_transformation['sourceColumns'][2]['entityName']+"""_"""+each_transformation['sourceColumns'][2]['columnName']+""" is null and """+each_transformation['sourceColumns'][3]['entityName']+"""_"""+each_transformation['sourceColumns'][3]['columnName']+""" is null then 'No'
            #         else 'Yes'
            #         end
            #     else
            #         case when """+each_transformation['sourceColumns'][0]['entityName']+"""_"""+each_transformation['sourceColumns'][0]['columnName']+""" = """+each_transformation['sourceColumns'][2]['entityName']+"""_"""+each_transformation['sourceColumns'][2]['columnName']+""" and """+each_transformation['sourceColumns'][1]['entityName']+"""_"""+each_transformation['sourceColumns'][1]['columnName']+""" = """+each_transformation['sourceColumns'][3]['entityName']+"""_"""+each_transformation['sourceColumns'][3]['columnName']+""" then 'Yes'
            #         else 'No'
            #         end
            #     end as """+each_transformation['attributeName']+""" from profiles
            #     """)
            query = """
                case when """+each_transformation['sourceColumns'][0]['entityName']+"""_"""+each_transformation['sourceColumns'][0]['columnName']+""" is null and """+each_transformation['sourceColumns'][1]['entityName']+"""_"""+each_transformation['sourceColumns'][1]['columnName']+""" is null then
                    case when """+each_transformation['sourceColumns'][2]['entityName']+"""_"""+each_transformation['sourceColumns'][2]['columnName']+""" is null and """+each_transformation['sourceColumns'][3]['entityName']+"""_"""+each_transformation['sourceColumns'][3]['columnName']+""" is null then 'No'
                    else 'Yes'
                    end
                else
                    case when """+each_transformation['sourceColumns'][0]['entityName']+"""_"""+each_transformation['sourceColumns'][0]['columnName']+""" = """+each_transformation['sourceColumns'][2]['entityName']+"""_"""+each_transformation['sourceColumns'][2]['columnName']+""" and """+each_transformation['sourceColumns'][1]['entityName']+"""_"""+each_transformation['sourceColumns'][1]['columnName']+""" = """+each_transformation['sourceColumns'][3]['entityName']+"""_"""+each_transformation['sourceColumns'][3]['columnName']+""" then 'Yes'
                    else 'No'
                    end
                end as """+each_transformation['attributeName']
            new_column_queries.append(query)
            ### Here <<<<<<<<<<<<<<<<<<<<<<<<--------------->>>>>>>>>>>>>>>>>> Checkpoint
            profile_df = spark.sql("""select *, {} from profiles""".format(", ".join(new_column_queries)))
            profile_df.cache()
            LOGGER.info("ccai - transformations - Checkpoint 1 : " + "{} x {}".format(profile_df.count(), len(profile_df.columns)))
            profile_df.createOrReplaceTempView("profiles")
            new_column_queries = list()
        elif each_transformation['transformationFunction'] == 'GetDetails':
            # profile_df = spark.sql("select *,case when IsRegistered = 'Yes' then " + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + " else " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " end as " + each_transformation['attributeName'] + " from profiles")
            query = "case when IsRegistered = 'Yes' then " + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + " else " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " end as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetDetailsPhone':
            # map[map_keys(map)[0]]
            personphonecolname = each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + \
                "[map_keys("+each_transformation['sourceColumns'][1]['entityName'] + \
                "_" + \
                each_transformation['sourceColumns'][1]['columnName']+")[0]]"
            # profile_df = spark.sql("select *, case when IsRegistered = 'Yes' then " + personphonecolname + " else " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " end as " + each_transformation['attributeName'] + " from profiles")
            query = "case when IsRegistered = 'Yes' then " + personphonecolname + " else " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " end as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetPassportNumber':
            passcol = each_transformation['transformationFunction'] + "_" + \
                each_transformation['sourceColumns'][0]['entityName'] + \
                "_" + each_transformation['sourceColumns'][0]['columnName']
            personcol = each_transformation['transformationFunction'] + "_" + \
                each_transformation['sourceColumns'][1]['entityName'] + \
                "_" + each_transformation['sourceColumns'][1]['columnName']
            # profile_df = spark.sql("select *, case when IsRegistered = 'Yes' then " + personcol + " else " + passcol + " end as " + each_transformation['attributeName'] + " from profiles")
            query = "case when IsRegistered = 'Yes' then " + personcol + " else " + passcol + " end as " + each_transformation['attributeName']
            new_column_queries.append(query)
            ### Here <<<<<<<<<<<<<<<<<<<<<<<<--------------->>>>>>>>>>>>>>>>>> Checkpoint
            profile_df = spark.sql("""select *, {} from profiles""".format(", ".join(new_column_queries)))
            profile_df.cache()
            LOGGER.info("ccai - transformations - Checkpoint 2 : " + "{} x {}".format(profile_df.count(), len(profile_df.columns)))
            profile_df.createOrReplaceTempView("profiles")
            new_column_queries = list()
        elif each_transformation['transformationFunction'] == 'GetStreetAddress':
            passcols = ", ".join([each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'],
                                  each_transformation['sourceColumns'][1]['entityName'] +
                                  "_" +
                                  each_transformation['sourceColumns'][1]['columnName'],
                                  each_transformation['sourceColumns'][2]['entityName'] +
                                  "_" +
                                  each_transformation['sourceColumns'][2]['columnName']
                                  ])
            personcols = ", ".join([each_transformation['sourceColumns'][3]['entityName'] + "_" + each_transformation['sourceColumns'][3]['columnName'],
                                    each_transformation['sourceColumns'][4]['entityName'] +
                                    "_" +
                                    each_transformation['sourceColumns'][4]['columnName'],
                                    each_transformation['sourceColumns'][5]['entityName'] + "_" + each_transformation['sourceColumns'][5]['columnName']])
            # profile_df = spark.sql("select *, case when IsRegistered = 'Yes' then " + "concat_ws(' ', " + personcols + ") else concat_ws(' ', " + passcols + ") end as " + each_transformation['attributeName'] + " from profiles")
            query = "case when IsRegistered = 'Yes' then " + "concat_ws(' ', " + personcols + ") else concat_ws(' ', " + passcols + ") end as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetGoIdFun':
            # profile_df = spark.sql("select *, case when IsRegistered = 'Yes' then " + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + "['OAFF'] else " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['OAFF'] end as " + each_transformation['attributeName'] + " from profiles")
            query = "case when IsRegistered = 'Yes' then " + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + "['OAFF'] else " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['OAFF'] end as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetSeniorCitizenID':
            # profile_df = spark.sql("select *, case when IsRegistered = 'Yes' then " + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + "['S'] else " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['S'] end as " + each_transformation['attributeName'] + " from profiles")
            query = "case when IsRegistered = 'Yes' then " + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + "['S'] else " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['S'] end as " + each_transformation['attributeName']
            new_column_queries.append(query)
            
        elif each_transformation['transformationFunction'] == 'GetBookedByAgentID' or each_transformation['transformationFunction'] == 'GetTravelSeat':
            # profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " as " +each_transformation['attributeName'] + " from profiles")
            profile_df = spark.sql("""select *, {} from profiles""".format(", ".join(new_column_queries)))
            profile_df.cache()
            LOGGER.info("ccai - transformations - Checkpoint 2 : " + "{} x {}".format(profile_df.count(), len(profile_df.columns)))
            profile_df.createOrReplaceTempView("profiles")
            new_column_queries = list()
            query = each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " as " +each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetTravelOriginAndDate':
            # profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " as " + each_transformation['attributeName'] + " from profiles")
            query = each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetTravelBaggageMealsInsurance':
            col_name = each_transformation['sourceColumns'][0]['entityName'] + \
                "_" + each_transformation['sourceColumns'][0]['columnName']
            # pull value from col_name if any of the keys BAG10, BAG15, BAG20, BAG25, BAG30, BAG32, BAG35, BAG40, BAG45
            if each_transformation['attributeName'] == 'TravelBaggage':
                # key_list_df = source_dict_copy["all_passengerfee"].select("FeeCode").dropDuplicates()#.join(source_dict_copy["all_fee"].select("FeeCode").dropDuplicates(), "FeeCode", "left")
                # key_list = key_list_df.select("FeeCode").rdd.map(lambda row: row[0]).collect()
                key_list = ['BAG35', 'BAG45', 'EXSBAG', 'PC32', 'BAG10', 'IBAG', 'XBAG', 'BAG25', 'BAGPRO', 'BAG32', 'PC20', 'XNBAG', 'PBAG',
                            'BAG30', 'GBAG', 'FLYBAG', 'BAG40', 'BAG20', 'OVSBAG', 'APTBAG', 'BAG15']
            elif each_transformation['attributeName'] == 'TravelMeals':
                # MEAL, MEAL1, MEAL2, MEAL3, MEAL4, MEAL5
                key_list = ['MEAL', 'MEAL1', 'MEAL2',
                            'MEAL3', 'MEAL4', 'MEAL5']
            elif each_transformation['attributeName'] == 'TravelInsurance':
                # NSURAD, NSURAR, NSURCD, NSURCR, NSURIN, NSURIR,ININF2, ININFO, ININFR, INS, INSCDO,INSCDR, INSLH1, INSLH2, INSOW, INSRT, INSRT2
                key_list = ['NSURAD', 'NSURAR', 'NSURCD', 'NSURCR', 'NSURIN', 'NSURIR', 'ININF2', 'ININFO',
                            'ININFR', 'INS', 'INSCDO', 'INSCDR', 'INSLH1', 'INSLH2', 'INSOW', 'INSRT', 'INSRT2']
            # sub_query = "select *, case "
            # for each_key in key_list:
            #     sub_query += "when " + col_name + \
            #         "['" + each_key + "'] is not null then " + \
            #         col_name + "['" + each_key + "'] "
            # sub_query += "else null end as " + \
            #     each_transformation['attributeName'] + " from profiles"
            # profile_df = spark.sql(sub_query)
            query = "case "
            for each_key in key_list:
                query += "when " + col_name + \
                    "['" + each_key + "'] is not null then " + \
                    col_name + "['" + each_key + "'] "
            query += "else null end as " + \
                each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetTravelGetGoKeyedIn':
            # select OAFF key
            # profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['OAFF'] as " + each_transformation['attributeName'] + " from profiles")
            query = each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['OAFF'] as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetIsPassenger':
            # if PassengerID is not null then yes else noCheckpoint
            # profile_df = spark.sql("select *, case when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " is not null then 'Yes' else 'No' end as " + each_transformation['attributeName'] + " from profiles")
            query = "case when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " is not null then 'Yes' else 'No' end as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetIsEmployee':
            # if IsRegistered = 'No' then if CEB is not null in Passcol then yes else if CEB is not null in personcolumn then yes else no
            # profile_df = spark.sql("""select *, case when IsRegistered = 'No' then
            # (case when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + """['CEB'] is not null then 'Yes'
            # else (case when """ + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + """['CEB'] is not null then 'Yes'
            # else 'No' end) end) else 'No' end as """ + each_transformation['attributeName'] + " from profiles")
            query = """case when IsRegistered = 'No' then
            (case when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + """['CEB'] is not null then 'Yes'
            else (case when """ + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + """['CEB'] is not null then 'Yes'
            else 'No' end) end) else 'No' end as """ + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetTravelDestination':
            ## Here <<<<<<<<<<---------->>>>>>>>>>>> checkpoint
            profile_df = spark.sql("""select *, {} from profiles""".format(", ".join(new_column_queries)))
            profile_df.cache()
            LOGGER.info("ccai - transformations - Checkpoint 3 : " + "{} x {}".format(profile_df.count(), len(profile_df.columns)))
            profile_df.cache()
            profile_df.createOrReplaceTempView("profiles")
            new_column_queries = list()
            # profile_df = spark.sql("select *, GetTravelDestination_" + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " as " + each_transformation['attributeName'] + " from profiles")
            query = "GetTravelDestination_" + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetIsEmployeeDependent':
            # if Firstname is equal to passcol1['CEB'] and LastName is equal to passcol2['CEB'] then yes else no
            # profile_df = spark.sql("""select *, case when IsRegistered = 'No' then
            # case when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + """['CEBD'] is not null then 'Yes'
            # else case when """ + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + """['CEBD'] is not null then 'Yes'
            # else 'No' end end else 'No' end as """ + each_transformation['attributeName'] + " from profiles")
            query = """case when IsRegistered = 'No' then
            case when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + """['CEBD'] is not null then 'Yes'
            else case when """ + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + """['CEBD'] is not null then 'Yes'
            else 'No' end end else 'No' end as """ + each_transformation['attributeName']
            new_column_queries.append(query)
            # LOGGER.info("GetIsEmployeeDependent completed")
        elif each_transformation['transformationFunction'] == 'GetBookerDetails':
            # profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " as " + each_transformation['attributeName'] + " from profiles")
            query = each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetBookerPassportNumber':
            ## Here <<<<<<<<<<---------->>>>>>>>>>>> checkpoint
            profile_df = spark.sql("""select *, {} from profiles""".format(", ".join(new_column_queries)))
            profile_df.cache()
            LOGGER.info("ccai - transformations - Checkpoint 4 : " + "{} x {}".format(profile_df.count(), len(profile_df.columns)))
            profile_df.createOrReplaceTempView("profiles")
            new_column_queries = list()
            col = "GetPassportNumber_" + \
                each_transformation['sourceColumns'][0]['entityName'] + \
                "_" + each_transformation['sourceColumns'][0]['columnName']
            # profile_df = spark.sql("select *, " + col + " as " +  each_transformation['attributeName'] + " from profiles")
            query = col + " as " +  each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetBookerType':
            # if col is 0 then None, if col is 1 then Customer, if col is 2 then Agent else null
            # profile_df = spark.sql("""select *, case when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + """ = 0 then 'None' when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + """ = 1 then 'Customer' when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + """ = 2 then 'Agent' else null end as """ + each_transformation['attributeName'] + " from profiles")
            query = "case when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + """ = 0 then 'None' when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + """ = 1 then 'Customer' when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + """ = 2 then 'Agent' else null end as """ + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetBookerMobile':
            # get col['M']
            # profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['M'] as " + each_transformation['attributeName'] + " from profiles")
            query = each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['M'] as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetBookerHomePhone':
            # get col['H']
            # profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" +  each_transformation['sourceColumns'][0]['columnName'] + "['H'] as " + each_transformation['attributeName'] + " from profiles")
            query = each_transformation['sourceColumns'][0]['entityName'] + "_" +  each_transformation['sourceColumns'][0]['columnName'] + "['H'] as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetBookerStreetAddress':
            # concat_ws col1, col2, col3
            # profile_df = spark.sql("select *, concat_ws(', ', " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + ", " + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + ", " + each_transformation['sourceColumns'][2]['entityName'] + "_" + each_transformation['sourceColumns'][2]['columnName'] + ") as " + each_transformation['attributeName'] + " from profiles")
            query = "concat_ws(', ', " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + ", " + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + ", " + each_transformation['sourceColumns'][2]['entityName'] + "_" + each_transformation['sourceColumns'][2]['columnName'] + ") as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetBookerGetGoID':
            # get col['GG']
            # profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['GG'] as " + each_transformation['attributeName'] + " from profiles")
            query = each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['GG'] as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetPrimaryNames':
            # get col['P']
            # profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['P'] as " + each_transformation['attributeName'] + " from profiles")
            query = each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['P'] as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetAdditionalNames':
            # get col['W']
            # profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['W'] as " + each_transformation['attributeName'] + " from profiles")
            query = each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['W'] as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetPrimaryContactStreetAddress':
            # concat_ws col1['P'], col2['P'], col3['P']
            # profile_df = spark.sql("select *, concat_ws(', ', " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['P'], " + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + "['P'], " + each_transformation['sourceColumns'][2]['entityName'] + "_" + each_transformation['sourceColumns'][2]['columnName'] + "['P']) as " + each_transformation['attributeName'] + " from profiles")
            query = "concat_ws(', ', " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['P'], " + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + "['P'], " + each_transformation['sourceColumns'][2]['entityName'] + "_" + each_transformation['sourceColumns'][2]['columnName'] + "['P']) as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetAdditionalContactStreetAddress':
            # concat_ws col1['W'], col2['W'], col3['W']
            # profile_df = spark.sql("select *, concat_ws(', ', " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['W'], " + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + "['W'], " + each_transformation['sourceColumns'][2]['entityName'] + "_" + each_transformation['sourceColumns'][2]['columnName'] + "['W']) as " + each_transformation['attributeName'] + " from profiles")
            query = "concat_ws(', ', " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['W'], " + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + "['W'], " + each_transformation['sourceColumns'][2]['entityName'] + "_" + each_transformation['sourceColumns'][2]['columnName'] + "['W']) as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation['transformationFunction'] == 'GetBookingChannel':
            # if col = 0 then Default if col = 1 then Direct if col = 2 then Web if col = 3 then GDS if col = 4 then API
            # profile_df = spark.sql("select *, case when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " = 0 then 'Default' when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " = 1 then 'Direct' when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " = 2 then 'Web' when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " = 3 then 'GDS' when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " = 4 then 'API' end as " + each_transformation['attributeName'] + " from profiles")
            query = "case when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " = 0 then 'Default' when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " = 1 then 'Direct' when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " = 2 then 'Web' when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " = 3 then 'GDS' when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " = 4 then 'API' end as " + each_transformation['attributeName']
            new_column_queries.append(query)
        elif each_transformation["transformationFunction"] == "GetPaymentMethodCode":
            # profile_df = spark.sql("select *, "+ each_transformation['sourceColumns'][0]['entityName']+"_" + each_transformation['sourceColumns'][0]['columnName']+" as "+each_transformation['attributeName']+" from profiles")
            query = each_transformation['sourceColumns'][0]['entityName']+"_" + each_transformation['sourceColumns'][0]['columnName']+" as "+each_transformation['attributeName']
            new_column_queries.append(query)
        # print(each_transformation)


    profile_df = spark.sql("""select *, {} from profiles""".format(", ".join(new_column_queries)))
    profile_df.cache()
    LOGGER.info("ccai - row count : " + str(profile_df.count()))
    return profile_df


# def transformations(config, profile_df, transaction_dict, spark,LOGGER,partition_date,source_dict_copy):
#     for each_column in config["columnMapping"]:
#         # each_column: {'sourceEntity': 'all_bookingpassenger', 'sourceColumn': 'PassengerID', 'targetColumn': 'PassengerID'}
#         profile_df.createOrReplaceTempView("profiles")
#         profile_df = spark.sql("select *, " + each_column['sourceEntity'] + "_" +
#                                each_column['sourceColumn'] + " as " + each_column['targetColumn'] + " from profiles")
        
#     for each_transformation in config["attributeMapping"]:
#         profile_df.createOrReplaceTempView("profiles")
#         if each_transformation['transformationFunction'] == 'GetTravelSoloOrGroup':
#             profile_df = spark.sql("select *,GetTravelSoloOrGroup_all_bookingpassenger_TravelSoloOrGroup  as " +
#                                    each_transformation['attributeName'] + " from profiles")
            

#         elif each_transformation['transformationFunction'] == 'GetPersonID':
#             profile_df = spark.sql("select *,all_person_PersonID as " +
#                                    each_transformation['attributeName'] + " from profiles")
            

#         elif each_transformation['transformationFunction'] == 'GetProvisionalPrimaryKey':
#             profile_df = spark.sql("select *,concat(all_bookingpassenger_PassengerID,'_',all_person_PersonID) as " +
#                                    each_transformation['attributeName'] + " from profiles")
            

#         elif each_transformation['transformationFunction'] == 'GetIsRegistered':
#             profile_df = spark.sql("""
#                 select *, 
#                 case when """+each_transformation['sourceColumns'][0]['entityName']+"""_"""+each_transformation['sourceColumns'][0]['columnName']+""" is null and """+each_transformation['sourceColumns'][1]['entityName']+"""_"""+each_transformation['sourceColumns'][1]['columnName']+""" is null then
#                     case when """+each_transformation['sourceColumns'][2]['entityName']+"""_"""+each_transformation['sourceColumns'][2]['columnName']+""" is null and """+each_transformation['sourceColumns'][3]['entityName']+"""_"""+each_transformation['sourceColumns'][3]['columnName']+""" is null then 'No'
#                     else 'Yes'
#                     end
#                 else
#                     case when """+each_transformation['sourceColumns'][0]['entityName']+"""_"""+each_transformation['sourceColumns'][0]['columnName']+""" = """+each_transformation['sourceColumns'][2]['entityName']+"""_"""+each_transformation['sourceColumns'][2]['columnName']+""" and """+each_transformation['sourceColumns'][1]['entityName']+"""_"""+each_transformation['sourceColumns'][1]['columnName']+""" = """+each_transformation['sourceColumns'][3]['entityName']+"""_"""+each_transformation['sourceColumns'][3]['columnName']+""" then 'Yes'
#                     else 'No'
#                     end
#                 end as """+each_transformation['attributeName']+""" from profiles
#                 """)
           

#         elif each_transformation['transformationFunction'] == 'GetDetails':
#             profile_df = spark.sql("select *,case when IsRegistered = 'Yes' then " +
#                                    each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + " else " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " end as " + each_transformation['attributeName'] + " from profiles")
            

#         elif each_transformation['transformationFunction'] == 'GetDetailsPhone':
#             # map[map_keys(map)[0]]
#             personphonecolname = each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + \
#                 "[map_keys("+each_transformation['sourceColumns'][1]['entityName'] + \
#                 "_" + \
#                 each_transformation['sourceColumns'][1]['columnName']+")[0]]"
#             profile_df = spark.sql("select *, case when IsRegistered = 'Yes' then " +
#                                    personphonecolname + " else " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " end as " + each_transformation['attributeName'] + " from profiles")
            

#         elif each_transformation['transformationFunction'] == 'GetPassportNumber':
#             passcol = each_transformation['transformationFunction'] + "_" + \
#                 each_transformation['sourceColumns'][0]['entityName'] + \
#                 "_" + each_transformation['sourceColumns'][0]['columnName']
#             personcol = each_transformation['transformationFunction'] + "_" + \
#                 each_transformation['sourceColumns'][1]['entityName'] + \
#                 "_" + each_transformation['sourceColumns'][1]['columnName']
#             profile_df = spark.sql("select *, case when IsRegistered = 'Yes' then " +
#                                    personcol + " else " + passcol + " end as " + each_transformation['attributeName'] + " from profiles")
           

#         elif each_transformation['transformationFunction'] == 'GetStreetAddress':
#             passcols = ", ".join([each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'],
#                                   each_transformation['sourceColumns'][1]['entityName'] +
#                                   "_" +
#                                   each_transformation['sourceColumns'][1]['columnName'],
#                                   each_transformation['sourceColumns'][2]['entityName'] +
#                                   "_" +
#                                   each_transformation['sourceColumns'][2]['columnName']
#                                   ])
#             personcols = ", ".join([each_transformation['sourceColumns'][3]['entityName'] + "_" + each_transformation['sourceColumns'][3]['columnName'],
#                                     each_transformation['sourceColumns'][4]['entityName'] +
#                                     "_" +
#                                     each_transformation['sourceColumns'][4]['columnName'],
#                                     each_transformation['sourceColumns'][5]['entityName'] + "_" + each_transformation['sourceColumns'][5]['columnName']])
#             profile_df = spark.sql("select *, case when IsRegistered = 'Yes' then " +
#                                    "concat_ws(' ', " + personcols + ") else concat_ws(' ', " + passcols + ") end as " + each_transformation['attributeName'] + " from profiles")
            

#         elif each_transformation['transformationFunction'] == 'GetGoIdFun':
#             profile_df = spark.sql("select *, case when IsRegistered = 'Yes' then " +
#                                    each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + "['OAFF'] else " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['OAFF'] end as " + each_transformation['attributeName'] + " from profiles")
            

#         elif each_transformation['transformationFunction'] == 'GetSeniorCitizenID':
#             profile_df = spark.sql("select *, case when IsRegistered = 'Yes' then " +
#                                    each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + "['S'] else " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['S'] end as " + each_transformation['attributeName'] + " from profiles")
            

#         elif each_transformation['transformationFunction'] == 'GetBookedByAgentID' or each_transformation['transformationFunction'] == 'GetTravelSeat':
#             profile_df = spark.sql("select *, " +
#                                    each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " as " +
#                                    each_transformation['attributeName'] + " from profiles")
            

#         elif each_transformation['transformationFunction'] == 'GetTravelOriginAndDate':
#             profile_df = spark.sql("select *, " +
#                                    each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " as " +
#                                    each_transformation['attributeName'] + " from profiles")
            

#         elif each_transformation['transformationFunction'] == 'GetTravelBaggageMealsInsurance':
#             col_name = each_transformation['sourceColumns'][0]['entityName'] + \
#                 "_" + each_transformation['sourceColumns'][0]['columnName']
#             # pull value from col_name if any of the keys BAG10, BAG15, BAG20, BAG25, BAG30, BAG32, BAG35, BAG40, BAG45
#             if each_transformation['attributeName'] == 'TravelBaggage':
#                 # key_list_df = source_dict_copy["all_passengerfee"].select("Feecode").dropDuplicates().join(source_dict_copy["all_fee"].select("Feecode").dropDuplicates(), "FeeCode", "left")

#                 # key_list_df = key_list_df.select("FeeCode")

#                 # key_list_df = key_list_df.filter((key_list_df["FeeCode"].like("%BAG%")) | (key_list_df["FeeCode"].like("%PC20%") | (key_list_df["FeeCode"].like("%PC32%"))))

#                 # key_list_df=key_list_df.select(key_list_df.FeeCode).distinct()
#                 # # Collect the matching values into a list
#                 # key_list = key_list_df.select("FeeCode").rdd.map(lambda row: row[0]).collect()
#                 key_list =['BAG35', 'BAG45', 'EXSBAG', 'PC32', 'BAG10', 'IBAG', 'XBAG', 'BAG25', 'BAGPRO', 'BAG32', 'PC20', 'XNBAG', 'PBAG',
#                             'BAG30', 'GBAG', 'FLYBAG', 'BAG40', 'BAG20', 'OVSBAG', 'APTBAG', 'BAG15']

#             elif each_transformation['attributeName'] == 'TravelMeals':
#                 # MEAL, MEAL1, MEAL2, MEAL3, MEAL4, MEAL5
#                 key_list = ['MEAL', 'MEAL1', 'MEAL2',
#                             'MEAL3', 'MEAL4', 'MEAL5']
#             elif each_transformation['attributeName'] == 'TravelInsurance':
#                 # NSURAD, NSURAR, NSURCD, NSURCR, NSURIN, NSURIR,ININF2, ININFO, ININFR, INS, INSCDO,INSCDR, INSLH1, INSLH2, INSOW, INSRT, INSRT2
#                 key_list = ['NSURAD', 'NSURAR', 'NSURCD', 'NSURCR', 'NSURIN', 'NSURIR', 'ININF2', 'ININFO',
#                             'ININFR', 'INS', 'INSCDO', 'INSCDR', 'INSLH1', 'INSLH2', 'INSOW', 'INSRT', 'INSRT2']
                
             
            
            
#             sub_query = "select *, case "
#             for each_key in key_list:
#                 sub_query += "when " + col_name + \
#                     "['" + each_key + "'] is not null then " + \
#                     col_name + "['" + each_key + "'] "
#             sub_query += "else null end as " + \
#                 each_transformation['attributeName'] + " from profiles"
#             profile_df = spark.sql(sub_query)
            


#         elif each_transformation['transformationFunction'] == 'GetTravelGetGoKeyedIn':
#             # select OAFF key
#             profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns']
#                                    [0]['columnName'] + "['OAFF'] as " + each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetIsPassenger':
#             # if PassengerID is not null then yes else no
#             profile_df = spark.sql("select *, case when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns']
#                                    [0]['columnName'] + " is not null then 'Yes' else 'No' end as " + each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetIsEmployee':
#             # if IsRegistered = 'No' then if CEB is not null in Passcol then yes else if CEB is not null in personcolumn then yes else no
#             profile_df = spark.sql("""select *, case when IsRegistered = 'No' then
#             case when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + """['CEB'] is not null then 'Yes'
#             else case when """ + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + """['CEB'] is not null then 'Yes'
#             else 'No' end end else 'No' end as """ + each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetTravelDestination':
#             profile_df = spark.sql("select *, GetTravelDestination_" + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns']
#                                    [0]['columnName'] + " as " + each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetIsEmployeeDependent':
#             # if Firstname is equal to passcol1['CEB'] and LastName is equal to passcol2['CEB'] then yes else no
#             profile_df = spark.sql("""select *, case when IsRegistered = 'No' then
#             case when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + """['CEBD'] is not null then 'Yes'
#             else case when """ + each_transformation['sourceColumns'][1]['entityName'] + "_" + each_transformation['sourceColumns'][1]['columnName'] + """['CEBD'] is not null then 'Yes'
#             else 'No' end end else 'No' end as """ + each_transformation['attributeName'] + " from profiles")
#             LOGGER.info("GetIsEmployeeDependent completed")
            
#         elif each_transformation['transformationFunction'] == 'GetBookerDetails':
#             profile_df = spark.sql("select *, " +
#                                    each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " as " +
#                                    each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetBookerPassportNumber':
#             col = "GetPassportNumber_" + \
#                 each_transformation['sourceColumns'][0]['entityName'] + \
#                 "_" + each_transformation['sourceColumns'][0]['columnName']
#             profile_df = spark.sql("select *, " + col + " as " +
#                                    each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetBookerType':
#             # if col is 0 then None, if col is 1 then Customer, if col is 2 then Agent else null
#             profile_df = spark.sql("""select *, case when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + """ = 0 then 'None' when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns']
#                                    [0]['columnName'] + """ = 1 then 'Customer' when """ + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + """ = 2 then 'Agent' else null end as """ + each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetBookerMobile':
#             # get col['M']
#             profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" +
#                                    each_transformation['sourceColumns'][0]['columnName'] + "['M'] as " + each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetBookerHomePhone':
#             # get col['H']
#             profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" +
#                                    each_transformation['sourceColumns'][0]['columnName'] + "['H'] as " + each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetBookerStreetAddress':
#             # concat_ws col1, col2, col3
#             profile_df = spark.sql("select *, concat_ws(', ', " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + ", " + each_transformation['sourceColumns'][1]['entityName'] + "_" +
#                                    each_transformation['sourceColumns'][1]['columnName'] + ", " + each_transformation['sourceColumns'][2]['entityName'] + "_" + each_transformation['sourceColumns'][2]['columnName'] + ") as " + each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetBookerGetGoID':
#             # get col['GG']
#             profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" +
#                                    each_transformation['sourceColumns'][0]['columnName'] + "['GG'] as " + each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetPrimaryNames':
#             # get col['P']
#             profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" +
#                                    each_transformation['sourceColumns'][0]['columnName'] + "['P'] as " + each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetAdditionalNames':
#             # get col['W']
#             profile_df = spark.sql("select *, " + each_transformation['sourceColumns'][0]['entityName'] + "_" +
#                                    each_transformation['sourceColumns'][0]['columnName'] + "['W'] as " + each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetPrimaryContactStreetAddress':
#             # concat_ws col1['P'], col2['P'], col3['P']
#             profile_df = spark.sql("select *, concat_ws(', ', " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['P'], " + each_transformation['sourceColumns'][1]['entityName'] + "_" +
#                                    each_transformation['sourceColumns'][1]['columnName'] + "['P'], " + each_transformation['sourceColumns'][2]['entityName'] + "_" + each_transformation['sourceColumns'][2]['columnName'] + "['P']) as " + each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetAdditionalContactStreetAddress':
#             # concat_ws col1['W'], col2['W'], col3['W']
#             profile_df = spark.sql("select *, concat_ws(', ', " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + "['W'], " + each_transformation['sourceColumns'][1]['entityName'] + "_" +
#                                    each_transformation['sourceColumns'][1]['columnName'] + "['W'], " + each_transformation['sourceColumns'][2]['entityName'] + "_" + each_transformation['sourceColumns'][2]['columnName'] + "['W']) as " + each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation['transformationFunction'] == 'GetBookingChannel':
#             # if col = 0 then Default if col = 1 then Direct if col = 2 then Web if col = 3 then GDS if col = 4 then API
#             profile_df = spark.sql("select *, case when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " = 0 then 'Default' when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " = 1 then 'Direct' when " + each_transformation['sourceColumns'][0]['entityName'] + "_" +
#                                    each_transformation['sourceColumns'][0]['columnName'] + " = 2 then 'Web' when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " = 3 then 'GDS' when " + each_transformation['sourceColumns'][0]['entityName'] + "_" + each_transformation['sourceColumns'][0]['columnName'] + " = 4 then 'API' end as " + each_transformation['attributeName'] + " from profiles")
            
#         elif each_transformation["transformationFunction"] == "GetPaymentMethodCode":
#             profile_df = spark.sql("select *, "+each_transformation['sourceColumns'][0]['entityName']+"_" +
#                                    each_transformation['sourceColumns'][0]['columnName']+" as "+each_transformation['attributeName']+" from profiles")
            
#         # print(each_transformation)
#     return profile_df

def join_ciam(config,profile_df,spark, partition_date,LOGGER):
    ciam_open_url = config['ciamPath'].replace("nonopen","open")

    ciam_open = f"{ciam_open_url}/ods={partition_date}/"
    df_ciam = spark.read.load(ciam_open, format="orc")

    
    df_ciam = df_ciam.drop("rowid", "s_startdt", "s_starttime", "s_enddt", "s_endtime", "s_deleted_flag", "c_journaltime", "c_transactionid", "c_operationtype", "c_userid", "ods")
    df_ciam = df_ciam.toDF(*["ciam_" + c for c in df_ciam.columns])
    df_ciam = df_ciam.withColumn("ciam_email", F.lower(F.col("ciam_email")))
    profile_df = profile_df.withColumn("BookerEmailAddress", F.lower(F.col("BookerEmailAddress")))
    profile_df.createOrReplaceTempView("profiles")
    df_ciam.createOrReplaceTempView("ciam")
    profile_df = spark.sql("select * from profiles left outer join ciam on profiles.BookerEmailAddress = ciam.ciam_email and profiles.IsRegistered='Yes'")
    # profile_not_null_df = profile_df.filter(profile_df.ciam_email.isNotNull())
    # profile_not_null_df.write.parquet("s3a://cebu-cdp-data-dev/ciam_validation", mode='overwrite')
    return profile_df
def profile_atomic_transformation(config, profile_df, spark):
    # apply regex on Phone column to remove all non-numeric characters
    profile_df = profile_df.withColumn(
        "Phone", F.regexp_replace("Phone", "[^0-9]", ""))
    return profile_df
def fillNa(config, profile_df, spark):
    # fill null values with empty string
    for each_column in config['profileSchema']:
        # if each_column['dataType'] == 'string':
        profile_df = profile_df.fillna(each_column['defaultValue'], each_column['columnName'])
    return profile_df


#change 2


# def get_df(config,spark,partition_date,archive_date,LOGGER):
#         # load open partitions - archive and a business date
#     open_dates = [partition_date,archive_date]

#     open_paths = []
#     for date in open_dates:
#         path = config["voucherPath"].replace("nonopen","open") + "/ods=" + date + "/"
#         open_paths.append(path)

#     try:
#         #try loading both partitions
#         voucher_df = spark.read.load(
#             open_paths, format="orc")
#     except:
#         #try loading business date partition
#         voucher_df = spark.read.load(
#             open_paths[0], format="orc")
        

#     #get available non open partitions
#     bucket,prefix = get_bucket(config,"all_voucher")
#     available_partitions = list_folders_in_path(bucket, prefix,LOGGER,archive_date,partition_date)
#     partition_paths = getpaths(available_partitions,"all_voucher",config["voucherPath"])


#     #load only if partitions are available
#     if partition_paths:
#         ##load non open partitions
#         non_open_df = spark.read.load(
#             partition_paths, format="orc").where('c_operationtype = "D"')
#         voucher_df = voucher_df.unionAll(non_open_df)

#     record_count = voucher_df.count()
#     LOGGER.info("ccai -> Load Data - {} : {} rows".format("voucher_df", record_count))
#     LOGGER.info("voucher_df loaded successfully")



#     return voucher_df


def add_csp(config, profile_df, spark, partition_date,LOGGER,source_dict_copy,archive_date):

    LOGGER.info("ccai - Processing CSP transactions")
    source_dict_copy["all_booking"].createOrReplaceTempView("booking")
    source_dict_copy["all_payment"].createOrReplaceTempView("payment")
    source_dict_copy["all_payment"].createOrReplaceTempView("voucher")

    query = "SELECT DISTINCT b.BookingID FROM booking b INNER JOIN payment p ON b.BookingID = p.BookingID AND p.PaymentMethodCode = 'VO' INNER JOIN voucher v ON v.VoucherReference = p.AccountNumber AND v.Amount > v.Available WHERE CAST(b.BookingUTC AS date) <= CAST(DATE_ADD(current_timestamp(), -1) AS DATE) AND b.status IN (2,3) AND v.voucherbasiscode IN ('99PHP', '99CSP2', '99CSP3', '99CSP4', '99CSP5') AND p.AuthorizationStatus = 4 AND v.status = 1;"
    csp_df = spark.sql(query)

    LOGGER.info("ccai - csp row count : " + str(csp_df.count()))

    csp_df = csp_df.withColumn("IsCsp", when(col("BookingID").isNotNull(), "yes").otherwise("no"))
    LOGGER.info("ccai - csp row count : " + str(csp_df.count()))

    profile_df = profile_df.join(csp_df,on="BookingID",how="left")
    #LOGGER.info("ccai - profile row count after csp join: " + str(profile_df.count()))
    profile_df = profile_df.withColumn("IsCsp", when(col("IsCsp") == 'yes', "yes").otherwise("no"))
    LOGGER.info("ccai - csp_check: " + str(profile_df.where("IsCsp = 'yes'").count()))
    
    return profile_df 



def ignore_bookings(source_dict_copy, profile_df, LOGGER):
    booking_df = source_dict_copy["all_booking"].select('BookingID', "RecordLocator")
    voucher_df = source_dict_copy["all_voucher"].select('RecordLocator')

    initial_row_count = profile_df.count()
    LOGGER.info("ccai - profile row count: {}".format(initial_row_count))

    ignore_bookings_df = booking_df.join(voucher_df, how="inner", on="RecordLocator")

    profile_df = profile_df.join(ignore_bookings_df, how="left_anti", on="BookingID")

    final_row_count_before_status_filter = profile_df.count()
    ignored_count = initial_row_count - final_row_count_before_status_filter

    LOGGER.info("ccai - CSP transactions ignored: {}".format(ignored_count))
    
    if ("Status" in profile_df.columns):
        profile_df=profile_df.select("*").where(profile_df.Status!=4)
        LOGGER.info("ccai - Status!=4 filtered in profiles")
        final_row_count_after_status_filter = profile_df.count()
        status_ignored_count = final_row_count_before_status_filter - final_row_count_after_status_filter
        LOGGER.info("ccai - Status!=4 ignored: {}".format(status_ignored_count))
    else:
        LOGGER.info("Status not found in profiles")


    return profile_df



def compute_profile(config_path, spark, partition_date, LOGGER):
    spark_conf(spark)
    LOGGER.info("ccai - ConfigPath : " + str(config_path))
    config = get_config(config_path)
    archive_date = "2021-01-01"
    LOGGER.info("ccai - profile loading complete")
    source_dict= load_data(config, spark,LOGGER,partition_date,archive_date)
    LOGGER.info("ccai - data loading complete")
    # LOGGER.info("ccai -> bookingid count after load data : {}".format(source_dict['all_booking'].select('BookingID').distinct().count()))
    
    source_dict_copy = source_dict.copy()
    source_dict = derived_tables(config, source_dict, spark, LOGGER)
    LOGGER.info("ccai - derived tables complete")
    # LOGGER.info("ccai -> bookingid count after derived tables : {}".format(source_dict['all_booking'].select('BookingID').distinct().count()))
    source_dict, transaction_dict = transaction_conversion(
        config, source_dict, spark, LOGGER)
    LOGGER.info("ccai - transaction conversion complete")
    # LOGGER.info("ccai -> bookingid count aftertransaction conversion : {}".format(source_dict['all_booking'].select('BookingID').distinct().count()))


    joined_df = join_entities(config, source_dict, spark, LOGGER)
    LOGGER.info("ccai - join entities complete")

    profile_column_list, profile_schema = create_profile(config)
    LOGGER.info("ccai - create profile complete")

    # profile_df = joined_df.join(profile_df, joined_df.all_bookingpassenger_PassengerID == profile_df.PassengerID, "left_outer")
    profile_df = transformationsNew(config, joined_df, transaction_dict, spark,LOGGER,partition_date,source_dict_copy)
    profile_df.cache()
    LOGGER.info("ccai - transformations complete")
    LOGGER.info("ccai - row count : " + str(profile_df.count()))



    # profile_df = profile_df.withColumnRenamed('all_booking_RecordLocator','RecordLocator')
    # profile_df = profile_df.withColumnRenamed('all_booking_Status','Status')

    

    profile_df = profile_df.select(profile_column_list)
    LOGGER.info("ccai - profile column subsetting complete")
    LOGGER.info("ccai - row count : " + str(profile_df.count()))


    profile_df = profile_atomic_transformation(config, profile_df, spark)
    LOGGER.info("ccai - atomic transformation complete")
    LOGGER.info("ccai - row count : " + str(profile_df.count()))
    # LOGGER.info("ccai -> bookingid count - check21 : {}".format(profile_df.select('BookingID').distinct().count()))

    ########### CheckPoints - Start #############
    # bookingIds_before = profile_df.select("ProvisionalPrimaryKey","BookingID").dropDuplicates().cache()
    # ppk_with_multiple_bookingId = profile_df.groupBy("ProvisionalPrimaryKey").agg(F.countDistinct("BookingID").alias("numBookingIds"), F.concat_ws(' | ',F.collect_set("BookingID")).alias("bookingIds")).filter("numBookingIds>1").cache()
    # ppk_with_multiple_bookingId_str = ppk_with_multiple_bookingId.toPandas().to_csv(index=False)
    # LOGGER.info("ccai - ProvisionalPrimaryKey With Multiple Booking Ids : " + str(ppk_with_multiple_bookingId.count()))
    # LOGGER.info("ccai - ProvisionalPrimaryKey With Multiple Booking Ids Df : \n" + ppk_with_multiple_bookingId_str)
    # LOGGER.info("ccai - Null ProvisionalPrimaryKey Records BookingId Count : " + str(profile_df.filter("(ProvisionalPrimaryKey is null) or (ProvisionalPrimaryKey='CCAI_NULL')").select("ProvisionalPrimaryKey","BookingID").dropDuplicates().count()))
    # LOGGER.info("ccai - Null ProvisionalPrimaryKeys Count : " + str(profile_df.select("ProvisionalPrimaryKey").dropDuplicates().count()))
    
    ########## CheckPoints-End #############


    # apply profile schema
    profile_df = profile_df.select(
        [F.col(c).cast(profile_schema[c]) for c in profile_schema])
    # prepare column list without ProvisionalPrimaryKey to get max value of each column
    col_list = [F.max(c).alias(c)
                for c in profile_df.columns if c != "ProvisionalPrimaryKey"]
    profile_df = profile_df.groupBy("ProvisionalPrimaryKey").agg(*col_list)
    profile_df.cache()
    LOGGER.info("ccai - Profile DF groupby Agg ")
    # LOGGER.info("ccai -> bookingid count - check21 : {}".format(profile_df.select('BookingID').distinct().count()))
    LOGGER.info("ccai - profile row count : " + str(profile_df.count()))

    profile_df = fillNa(config, profile_df, spark)
    profile_df = join_ciam(config, profile_df, spark, partition_date,LOGGER)
    LOGGER.info("ccai - join ciam complete")
    LOGGER.info("ccai - row count : " + str(profile_df.count()))
    # LOGGER.info("ccai -> bookingid count - check22 : {}".format(profile_df.select('BookingID').distinct().count()))

    ########### CheckPoints - Start #############
    # bookingIds_after = profile_df.select("ProvisionalPrimaryKey","BookingID").dropDuplicates()
    # lost_bookingIds = bookingIds_before.join(bookingIds_after, ["ProvisionalPrimaryKey","BookingID"],"left_anti").toPandas().to_csv(index=False)
    # LOGGER.info("ccai - Null ProvisionalPrimaryKeys Count : " + str(profile_df.select("ProvisionalPrimaryKey").dropDuplicates().count()))
    # LOGGER.info("ccai - Lost Booking Ids Df : \n" + lost_bookingIds)
    # raise SystemExit(0)

    # import sys
    # sys.exit(0)
    ########## CheckPoints-End #############
    
    # change 1
    profile_df = profile_df.dropDuplicates(subset=["PassengerID"])
    LOGGER.info("ccai - profile row count: " + str(profile_df.count()))

    profile_df = add_csp(config, profile_df, spark, partition_date,LOGGER,source_dict_copy,archive_date)
    LOGGER.info(f"ccai csp attribute added")
    
    profile_df = ignore_bookings(source_dict_copy, profile_df,LOGGER)
    LOGGER.info(f"bookings ignored")
    LOGGER.info("ccai - profile row count: " + str(profile_df.count()))


    
    profile_df.cache()
    save_path = config['storageDetails'][0]['pathUrl']
    ucp_path = config['storageDetails'][2]['pathUrl'] + \
        "/" + "p_date=" + partition_date
    profile_df.write.mode("overwrite").parquet(save_path)

    # profile_df_10rowsStr = spark.read.parquet(save_path).limit(10).toPandas().to_csv()
    # LOGGER.info("ccai - sample 10 rows : \n"+ str(profile_df_10rowsStr))
    return save_path, ucp_path
