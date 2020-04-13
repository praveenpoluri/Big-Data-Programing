import csv
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from typing import Any

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("nullValue", "NA").option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss").option("mode", "failfast").load("C:\\Users\\prave\\OneDrive\\Documents\\GitHub\\Big-Data-Programing\\Spark-Data-Frame\\survey.csv")  # type: Any
df.createOrReplaceTempView("Survey")
print(df.dropDuplicates().count())
