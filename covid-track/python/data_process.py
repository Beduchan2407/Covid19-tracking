import findspark
findspark.init()
import pandas as pd
from datetime import datetime
import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.types import *


spark = SparkSession.builder\
        .appName("data-process")\
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

# read file from local machine
df = spark.read\
    .format("csv")\
    .option("path", "s3a://covid19-us-tracking/daily-covid-data/*/*/*/*.csv")\
    .option("header", "true")\
    .load()

# delete null value
df = df.na.drop()

# create Location_dim table
location_dim = df.select("county", "fips_code", "state", "total_population").distinct()
location_dim = location_dim.withColumn("location_key", f.row_number().over(Window.orderBy("state", "county", "fips_code","total_population")))

# create date_dim table
date_dim = df.select("date")\
    .withColumn("year", f.year("date"))\
    .withColumn("month", f.month("date"))\
    .withColumn("day", f.dayofmonth("date")).distinct()
date_dim = date_dim.withColumn("date_key", f.row_number().over(Window.orderBy("date")))

# create fact table
covid_daily_fact = df.join(location_dim, (df.county == location_dim.county) & (df.fips_code == location_dim.fips_code) & (df.state == location_dim.state) & (df.total_population == location_dim.total_population), "inner")\
    .join(date_dim, (df.date == date_dim.date), "inner")\
    .select("location_key", "date_key", "cumulative_cases", "cumulative_deaths", "new_cases", "new_deaths")

location_dim.repartition(1)\
            .select("location_key", "state", "county", "fips_code", "total_population")\
            .toPandas().to_csv("s3://covid19-us-tracking/data_process_output/location_dim.csv", header=True, mode="w", index=False, storage_options={"key": AWS_ACCESS_KEY_ID, "secret": AWS_SECRET_ACCESS_KEY})

date_dim.repartition(1)\
    .select("date_key", "date", "year", "month", "day")\
    .toPandas().to_csv("s3://covid19-us-tracking/data_process_output/date_dim.csv", header=True, mode="w", index=False, storage_options={"key": AWS_ACCESS_KEY_ID, "secret": AWS_SECRET_ACCESS_KEY})

covid_daily_fact.repartition(1)\
    .select("location_key", "date_key", "cumulative_cases", "cumulative_deaths", "new_cases", "new_deaths")\
    .toPandas().to_csv("s3://covid19-us-tracking/data_process_output/covid_daily_fact.csv", header=True, mode="w", index=False, storage_options={"key": AWS_ACCESS_KEY_ID, "secret": AWS_SECRET_ACCESS_KEY})

