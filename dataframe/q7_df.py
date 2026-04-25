from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Q7 DF").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

# Compute average fare
avg_fare = df.agg({"fare_amount": "avg"}).collect()[0][0]

# Filter above average
df_q7 = df.filter(col("fare_amount") > avg_fare)

df_q7.show()
df_q7.explain(True)
