import configparser
import os
import pandas as pd
from pyspark.sql import SparkSession 
from pyspark.sql.types import IntegerType, DoubleType 
from pyspark.sql.functions import col, asc, desc, udf, to_date
import datetime as dt


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: 
        - creates Spark session

    Arguments:
        None

    Returns:
        - Spark session object
    """
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark


def process_demographic_data(spark, input_data, output_data):
    """
    Description: 
        - extracts, transforms and loads demographic data into S3 data lake

    Arguments:
        spark: Spark session
        input_data: location of demographic data
        output_data: location of S3 data lake

    Returns:
        None
    """
   
    # read demographic data file
    df_spark=spark.read.option("header", True).options(delimiter=';').csv("us-cities-demographics.csv")

    # convert data types
    df_spark = df_spark.withColumn("Median Age", df_spark["Median Age"].cast(DoubleType())) \
        .withColumn("Male Population", df_spark["Male Population"].cast(IntegerType())) \
        .withColumn("Female Population", df_spark["Female Population"].cast(IntegerType())) \
        .withColumn("Total Population", df_spark["Total Population"].cast(IntegerType())) \
        .withColumn("Number of Veterans", df_spark["Number of Veterans"].cast(IntegerType())) \
        .withColumn("Foreign-born", df_spark["Foreign-born"].cast(IntegerType())) \
        .withColumn("Average Household Size", df_spark["Average Household Size"].cast(DoubleType())) \
        .withColumn("Count", df_spark["Count"].cast(IntegerType()))


    # rename fields, sort by city, state, and race count, and then drop duplicates based on city & state
    df_spark = df_spark.select(col('City').alias('city'), \
        col('State Code').alias('state_code'), \
        col('Median Age').alias('median_age'), \
        col('Male Population').alias('male_population'), \
        col('Female Population').alias('female_population'), \
        col('Total Population').alias('total_population'), \
        col('Number of Veterans').alias('veteran_population'), \
        col('Foreign-born').alias('foreign_population'), \
        col('Average Household Size').alias('avg_household_size'), \
        col('Race').alias('race_majority'), \
        col('Count').alias('race_count')) \
        .orderBy(col('city').asc(),col('state_code').asc(),col('race_count').desc()) \
        .dropDuplicates(['city','state_code'])

 
    # write demographic table to parquet files
    df_spark.write.mode('overwrite').parquet(os.path.join(output_data, 'capstone/demographic.parquet'))


def process_immigration_data(spark, input_data, output_data):
    """
    Description: 
        - extracts, transforms and loads S3 log data into S3 data lake

    Arguments:
        spark: Spark session
        input_data: location of S3 log data
        output_data: location of S3 data lake

    Returns:
        None
    """

    # read immigration data
    df_spark=spark.read.parquet("sas_data")
    
    # convert data types
    df_spark = df_spark.withColumn("cicid", df_spark["cicid"].cast(IntegerType())) \
        .withColumn("i94cit", df_spark["i94cit"].cast(IntegerType())) \
        .withColumn("i94res", df_spark["i94res"].cast(IntegerType())) \
        .withColumn("i94mode", df_spark["i94mode"].cast(IntegerType())) \
        .withColumn("i94bir", df_spark["i94bir"].cast(IntegerType())) \
        .withColumn("i94visa", df_spark["i94visa"].cast(IntegerType())) \
        .withColumn("biryear", df_spark["biryear"].cast(IntegerType()))


    # rename columns and remove unneeded columns    
    df_spark = df_spark.select(col('cicid').alias('immigration_id'), \
                               col('i94cit').alias('citizenship_country_id'), \
                               col('i94res').alias('residency_country_id'), \
                               col('i94port').alias('arrival_city_id'), \
                               col('arrdate').alias('arrival_date'), \
                               col('i94mode').alias('travel_mode_id'), \
                               col('i94addr').alias('current_state_id'), \
                               col('depdate').alias('departure_date'), \
                               col('i94bir').alias('immigration_age'), \
                               col('i94visa').alias('visa_type_id'), \
                               col('biryear').alias('birth_year'), \
                               col('gender'))


    # convert date fields to date type
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(float(x))).isoformat() if x else None)
    
    df_spark = df_spark.withColumn("arrival_date", get_date(df_spark.arrival_date))
    df_spark = df_spark.withColumn("departure_date", get_date(df_spark.departure_date))

    df_spark = df_spark.withColumn("arrival_date", to_date(df_spark.arrival_date, 'yyyy-MM-dd')).withColumn("departure_date", to_date(df_spark.departure_date, 'yyyy-MM-dd'))
    
    # duplicate arrival_date column to partition by
    df_spark = df_spark.withColumn("arrival_date_partition", df_spark["arrival_date"])
    
    # write immigration table to parquet files partitioned by arrival date
    df_spark.write.mode('overwrite').partitionBy('arrival_date_partition').parquet(os.path.join(output_data, 'capstone/immigration.parquet'))

    # create table of unique date values
    df_spark_date1 = df_spark.select(col('arrival_date').alias('date_id')).dropDuplicates() 
    df_spark_date2 = df_spark.select(col('departure_date').alias('date_id')).dropDuplicates() 
    date_df = df_spark_date1.union(df_spark_date2).distinct()

    # write date table to parquet files
    date_df.write.mode('overwrite').parquet(os.path.join(output_data, 'capstone/date.parquet'))


def main():
    """
    Description: 
        - creates a Spark session
        - calls process_demographic_data to extract, transform and load demographic data into S3 data lake
        - calls process_immigration_data to extract, transform and load immigration data into S3 data lake
        - closes connection

    Arguments:
        None 

    Returns:
        None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://lhemelt/"
    
    process_demographic_data(spark, input_data, output_data)    
    process_immigration_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
