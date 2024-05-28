from sessionCreation import *

# Spark session created.
spark = sparkSession()

input_path = "s3://mygluecicd-20240528-01/input/OfficeDataProject.csv"
df = spark.read.format('csv').option('header','ture').load(input_path)

output_path = "s3://mygluecicd-20240528-01/input/output.json"
df.coalesce(1).write.format('json').mode('overwrite').save(output_path)
