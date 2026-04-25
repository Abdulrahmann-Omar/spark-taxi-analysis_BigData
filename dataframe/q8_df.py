from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("Q8 DF").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

# Create small dataset
small_df = df.select("pickup_longitude", "pickup_latitude").distinct().limit(100)

# Force Sort-Merge Join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

start = time.time()

df_q8 = df.join(
    small_df,
    on=["pickup_longitude", "pickup_latitude"],
    how="inner"
)

df_q8.count()

print("Sort-Merge Join Time:", time.time() - start)

df_q8.explain(True)
