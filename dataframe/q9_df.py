from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import time

spark = SparkSession.builder.appName("Q9 DF").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

small_df = df.select("pickup_longitude", "pickup_latitude").distinct().limit(100)

start = time.time()

df_q9 = df.join(
    broadcast(small_df),
    on=["pickup_longitude", "pickup_latitude"],
    how="inner"
)

df_q9.count()

print("Broadcast Join Time:", time.time() - start)

df_q9.explain(True)
