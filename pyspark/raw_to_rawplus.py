from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, DataFrame
from pypsark import SparkContext, SparkConf
from pyspark import SparkFiles
from pyspark.sql import functions as F

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

def input_read():
    df = spark.read.option(header='True', inferSchema='True', delimiter=',') \
                      .csv("s3://mer-dlake-raw/data/raw/").withColumn("filename", F.input_file_name())
    logger.info("Input files loaded")

    return df

def raw_col_rename(input_df):
    #df = input_df.withColumnRenamed("Origin Movement ID","origin_movement_id")  \
    #       .withColumnRenamed("Origin Display Name","origin_display_name")      \                         
    #       .withColumnRenamed("Origin Geometry","origin_geometry")              \
    #       .withColumnRenamed("Destination Movement ID","destination_movement_id")      \
    #       .withColumnRenamed("Destination Display Name","destination_display_name")    \
    #       .withColumnRenamed("Destination Geometry","destination_geomtry")             \
    #       .withColumnRenamed("Date Range","date_range")                                \
    #       .withColumnRenamed("Mean Travel Time (Seconds)","mean_travel_time_seconds")          \
    #       .withColumnRenamed("Range - Lower Bound Travel Time (Seconds)","lower_bound_travel_time_seconds")  \
    #       .withColumnRenamed("Range - Upper Bound Travel Time (Seconds)","upper_bound_travel_time_seconds")  
    
    input_columns = input_df.columns()
    output_columns = [ for cols in input_columns ]
    for col in input_df.columns:
        df = input_df.withColumnRenamed( col, col.lower().replace(" ","_").replace("(","").replace(")","") )
    logger.info("Raw column rename done")

    return df


def raw_col_ts(input_df):
    df =  input.withColumn("rawload_timestamp",current_ts())
    logger.info("Added Raw load timestamp")
    
    return df


def main():
    try:
        #Initializing spark session
        spark = SparkSession.builder \
                    .appName("Uber ingestion") \
                    .enableHiveSupport() \
                    .getOrCreate()
        
        #Initializing logger
        logger=setLogger(spark)
        logger.info("Spark job started")

        #Read input csv from S3
        df = input_read()
        logger.info("Input read initiated")
        
        #Standardization of output column names
        df = raw_col_rename(df)
        logger.info("Input column rename initiated")
        
        #Append Raw execution timestamp
        df = raw_col_ts(df)
        logger.info("Append Raw timestamp initiated")
        
        #input record count
        input_count = df.count()
        logger.info('input record count: '+str(input_count))
        
        if ( input_count > 0 ):
            df.coalesce(1).write.mode("append").parquet("s3://mer-dlake-rawplus/data/rawplus/")
            logger.info('Data write to target S3 location complete')
        
        else:
            logger.info('No data to write to target')

    
    except Exception as e:
        logger.error('Error occured: '+str(e))
        raise(e)

    finally:
        spark.stop()

if _name_ == '__main__':
    main()
        
