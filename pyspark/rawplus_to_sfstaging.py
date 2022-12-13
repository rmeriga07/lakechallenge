from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, DataFrame
from pypsark import SparkContext, SparkConf
from pyspark import SparkFiles
from pyspark.sql import functions as F
from pyspark.sql.functions import split

import boto3
import sys

from datetime import date, datetime, timedelta
import os
import json
import time
import uuid

def setLogger(spark):
    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger("SparkLogger")

def ts():
    return datetime.now()

def current_ts():
    return ts().strftime('%Y-%m-%D %H-%M-%S')

def current_ts_dir():
    return ts().strftime('%Y%m%D_%H%M%S')

def rawplus_read():
    df = spark.read.parquet("s3://mer-dlake-rawplus/data/rawplus/")
    logger.info("Raw plus loaded")

    return df

def sf_transformation(input_df):
    df = input_df.withColumn("dates", split(col("date_range"), ",").getItem(0) )  \
                 .withColumn("start_date", split(col("dates"), "-").getItem(0) )  \
                 .withColumn("end_date", split(col("dates"), "-").getItem(1) )  \ 
                 .withColumn("city", split(col("filename"), "-").getItem(1) )  \ 
                 .drop("dates")                  
    #       .withColumnRenamed("Origin Geometry","origin_geometry")              \
    #       .withColumnRenamed("Destination Movement ID","destination_movement_id")      \
    #       .withColumnRenamed("Destination Display Name","destination_display_name")    \
    #       .withColumnRenamed("Destination Geometry","destination_geomtry")             \
    #       .withColumnRenamed("Date Range","date_range")                                \
    #       .withColumnRenamed("Mean Travel Time (Seconds)","mean_travel_time_seconds")          \
    #       .withColumnRenamed("Range - Lower Bound Travel Time (Seconds)","lower_bound_travel_time_seconds")  \
    #       .withColumnRenamed("Range - Upper Bound Travel Time (Seconds)","upper_bound_travel_time_seconds")  
    
    
    logger.info("Snowflake column transformation done")

    return df


def sf_col_ts(input_df):
    df =  input.withColumn("sf_timestamp",current_ts())
    logger.info("Added Snowflake load timestamp")
    
    return df


def main():
    try:
        #Initializing spark session
        spark = SparkSession.builder \
                    .appName("Uber transformation") \
                    .enableHiveSupport() \
                    .getOrCreate()
        
        #Initializing logger
        logger=setLogger(spark)
        logger.info("Spark job started")

        #Read Rawplus data from S3
        df = rawplus_read()
        logger.info("Rawplus read initiated")
        
        #Transform of columns for Snowflake
        df = sf_transformation(df)
        logger.info("Snowflake transformation initiated")
        
        #Append Raw execution timestamp
        df = sf_col_ts(df)
        logger.info("Append Snowflake timestamp initiated")
        
        #staging record count
        stg_count = df.count()
        logger.info('staging record count: '+str(stg_count))
        
        if ( stg_count > 0 ):
            df.coalesce(1).write.mode("overwrite").parquet("s3://mer-dlake-stg/data/sf/")
            logger.info('Data write to Snowflake staging location complete')
        
        else:
            logger.info('No data to write to Snowflake stage')

    
    except Exception as e:
        logger.error('Error occured: '+str(e))
        raise(e)

    finally:
        spark.stop()

if _name_ == '__main__':
    main()
        
