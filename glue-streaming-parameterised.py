import sys
from awsglue import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Row
import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TOPICS'])
TOPICS=args['TOPICS']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

frame0 = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "server1:9094,server2:9094,server3:9094") \
  .option("subscribe", TOPICS) \
  .option("startingOffsets", "earliest") \
  .load()

frame1 = frame0 \
  .selectExpr("topic", "offset", "timestamp", "CAST(value AS STRING) as value") \
    
# To convert the script into the streaming job,
# update the trigger below from 'once=True' to:
# .trigger(processingTime="60 seconds") and 
# re-deploy the script as gluestreaming job
frame2 = frame1 \
  .writeStream \
  .format("csv") \
  .option("checkpointLocation", "s3://<output-bucket>/kafka/checkpoint/") \
  .option("path", "s3://<output-bucket>/kafka/data/") \
  .partitionBy("topic") \
  .outputMode("append") \
  .trigger(once=True) \
  .start() \
  .awaitTermination()

job.commit()
