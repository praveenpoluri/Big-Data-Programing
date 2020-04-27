import os
os.environ["PYSPARK_SUBMIT_ARGS"]  = ("--packages  graphframes:graphframes:0.6.0-spark2.4.5-s_2.12.0 pyspark-shell")
from graphframes import *
from pyspark.sql import SparkSession