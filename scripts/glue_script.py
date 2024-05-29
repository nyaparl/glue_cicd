from sessionCreation import *

# Spark session created.
spark = sparkSession()

input_path = "s3://mybucket-20240529-new/input/sample.csv"
df = spark.read.format('csv').option('header','ture').load(input_path)

output_path = "s3://mybucket-20240529-new/input/sample.json"
df.coalesce(1).write.format('json').mode('overwrite').save(output_path)
