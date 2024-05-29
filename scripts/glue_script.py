import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Spark session created.
spark = SparkSession.builder.appName('first').master('local[*]').getOrCreate()

input_path = "s3://mybucket-20240529-new/input/sample.csv"
df = spark.read.format('csv').option('header','ture').load(input_path)

output_path = "s3://mybucket-20240529-new/input/sample.json"
df.coalesce(1).write.format('json').mode('overwrite').save(output_path)
