from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Q2 DF").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

df_q2 = df.filter(
    (col("trip_distance") > 5) &
    (col("fare_amount") > 20)
)

df_q2.show()
df_q2.explain(True)
