import findspark
findspark.init()
import pandas as pd
from datetime import datetime
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *


spark = SparkSession.builder\
        .appName("data-ingestion")\
        .getOrCreate()

os.environ["AWS_ACCESS_KEY_ID"] = sys.argv[1]
os.environ["AWS_SECRET_ACCESS_KEY"] = sys.argv[2]
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.access.key", AWS_ACCESS_KEY_ID)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.secret.key", AWS_SECRET_ACCESS_KEY)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3ai.endpoint", "s3.amazonaws.com")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.endpoint", "s3.amazonaws.com")

schema = StructType([
    StructField("uid", StringType()),
    StructField("location_type", StringType()),
    StructField("fips_code", IntegerType()),
    StructField("state", StringType()),
    StructField("date", DateType()),
    StructField("total_population", IntegerType()),
    StructField("cumulative_cases", IntegerType()),
    StructField("cumulative_cases_per_100_100", IntegerType()),
    StructField("cumulative_deaths", IntegerType()),
    StructField("cumulative_deaths_per_100_100", IntegerType()),
    StructField("new_cases", IntegerType()),
    StructField("new_deaths", IntegerType()),
    StructField("new_cases_per_100_100", IntegerType()),
    StructField("new_deahs_per_100_100", IntegerType()),
    StructField("new_cases_7_day_rolling_avg", FloatType()),
    StructField("new_deaths_7_day_rolling_avg", FloatType())
])

# read file from local machine
df = spark.read\
    .format("csv")\
    .option("path", "file:///tmp/covid_data/data.csv")\
    .option("header", "true")\
    .load()


# Check folder is exist or not
sc = spark.sparkContext
jvm = sc._jvm
conf = sc._jsc.hadoopConfiguration()
bucket_url = "s3a://covid19-us-tracking/daily-covid-data/_SUCCESS"
uri = jvm.java.net.URI(bucket_url)
fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
exist = fs.exists(jvm.org.apache.hadoop.fs.Path(bucket_url))

if exist:
    # incremental load
    s3_df = spark.read\
        .format("csv")\
        .option("header", "true")\
        .option("path", "s3a://covid19-us-tracking/daily-covid-data/*/*/*/*.csv")\
        .load()
    last_date = s3_df.agg(f.max(f.col("date"))).head()[0]
    output = df\
        .select(f.col("location_name").alias("county"), "fips_code", "state", "date", "total_population", "cumulative_cases", "cumulative_deaths", "new_cases", "new_deaths")\
        .withColumn("year", f.year("date"))\
        .withColumn("month", f.month("date"))\
        .withColumn("day", f.dayofmonth("date"))\
        .where(f.col("date") > datetime.strptime(last_date, "%Y-%m-%d"))\
        .orderBy("date")
else:
    # intial load
    output = df\
        .select(f.col("location_name").alias("county"), "fips_code", "state", "date", "total_population", "cumulative_cases", "cumulative_deaths", "new_cases", "new_deaths")\
        .withColumn("year", f.year("date"))\
        .withColumn("month", f.month("date"))\
        .withColumn("day", f.dayofmonth("date"))\
        .orderBy("date")

output = output.repartition(3)

output.write\
    .format("csv")\
    .partitionBy("year", "month", "day")\
    .option("header", "true")\
    .option("path", "s3a://covid19-us-tracking/daily-covid-data")\
    .mode("append")\
    .save()
