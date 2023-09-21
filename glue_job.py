import sys
from awsglue.transforms import Join
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
import py4j
from ccai import customer360ai
# import redshift_connector
import pandas as pd
import json
import boto3
from botocore.exceptions import ClientError
def get_bucket_and_prefix(s3_url):
    bucket = s3_url.split('/')[2]
    prefix = '/'.join(s3_url.split('/')[3:])
    return bucket, prefix
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.info("pyspark script logger initialized")
ccaiInstance = customer360ai()
LOGGER.info("ccai instance created")

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'config_path',
                           'partition_value',
                            'checkpoint_dir',
                            'TempDir',
                            'secret_name',
                            'secret_region',
                            'redshift_database',
                            'redshift_profile_table',
                            'redshift_passenger_index',
                           ])

def get_secret(secret_name, region_name):    
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return secret
# res = json.loads(get_secret(secret_name=args['secret_name'], region_name=args['secret_region']))
# db_name = args["redshift_database"]
# passenger_table = args["redshift_passenger_index"]
# profile_table = args["redshift_profile_table"]



# db_url = "jdbc:redshift://"+res['host'] + ":" + str(res['port']) + "/" + db_name
profile_path,ucp_path = ccaiInstance.compute_profile(
    config_path=args['config_path'], spark=spark, partition_date=args['partition_value'], logger=LOGGER)

# ucp_path = "s3a://cebu-cdp-data-dev/ucp/p_date=2023-06-01"
# profile_path = "s3a://cebu-cdp-data-dev/profiles"
# dedupe_path = "s3a://cebu-cdp-data-dev/dedupe"
dedupe_path = ccaiInstance.compute_dedupe(
    config_path=args['config_path'], spark=spark, partition_date=args['partition_value'], logger=LOGGER)

ucp_path = ccaiInstance.compute_ucp(
    config_path=args['config_path'], profile_path=profile_path, dedupe_path=dedupe_path, ucp_path=ucp_path, spark=spark, partition_date=args['partition_value'], logger=LOGGER)


ccaiInstance.compute_ui(args['config_path'], spark, ucp_path, LOGGER)

#! Temporary disable writing to redshift due to space issues on dev redshift in cebu
# df_profile = DynamicFrame.fromDF(df_profile, glueContext, "profiles")
# df_dedupe = DynamicFrame.fromDF(df_dedupe, glueContext, "passenger_hash")
# LOGGER.info("ccai truncating profile table")
# conn = redshift_connector.connect(
#     host=res['host'],
#     port=res['port'],
#     user=res['username'],
#     password=res['password'],
#     database=db_name
# )
# cursor = conn.cursor()
# cursor.execute("truncate table " + profile_table)
# conn.commit()
# cursor.close()
# conn.close()
# LOGGER.info("ccai truncating profile table done. writing to redshift")

# my_conn_options = {
#     "url": db_url,
#     "dbtable": args["redshift_profile_table"],
#     "user": res['username'],
#     "password": res['password'],
#     "redshiftTmpDir": args["TempDir"],
# }
# glueContext.write_dynamic_frame.from_options(
#     frame=df_profile,
#     connection_type="redshift",
#     connection_options=my_conn_options,
# )
# LOGGER.info("ccai writing profile to redshift done")


# LOGGER.info("ccai truncating passenger table")
# conn = redshift_connector.connect(
#     host=res['host'],
#     port=res['port'],
#     user=res['username'],
#     password=res['password'],
#     database=db_name
# )
# cursor = conn.cursor()
# cursor.execute("truncate table " + passenger_table)
# conn.commit()
# cursor.close()
# conn.close()
# LOGGER.info("ccai truncating passenger table done. writing to redshift")
# my_conn_options = {
#     "url": db_url,
#     "dbtable": args["redshift_passenger_index"],
#     "user": res['username'],
#     "password": res['password'],
#     "redshiftTmpDir": args["TempDir"],
# }
# glueContext.write_dynamic_frame.from_options(
#     frame=df_dedupe,
#     connection_type="redshift",
#     connection_options=my_conn_options,
# )
# LOGGER.info("ccai writing passenger to redshift done")
# s3 = boto3.resource('s3')
# bucket_url,prefix_url = get_bucket_and_prefix(args["TempDir"])
# bucket = s3.Bucket(bucket_url)
# bucket.objects.filter(Prefix=prefix_url).delete()
# LOGGER.info("ccai cleaning up temp dir done")
