from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import time

spark = SparkSession.builder.appName("Join Optimization").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

small_df = df.select("pickup_longitude", "pickup_latitude").distinct().limit(100)

# Sort-Merge Join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

start = time.time()
df.join(small_df, ["pickup_longitude", "pickup_latitude"]).count()
print("SortMergeJoin:", time.time() - start)

# Broadcast Join
start = time.time()
df.join(broadcast(small_df), ["pickup_longitude", "pickup_latitude"]).count()
print("BroadcastJoin:", time.time() - start)
