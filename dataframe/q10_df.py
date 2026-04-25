from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("Q10 DF").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

# Without cache
start = time.time()
df.groupBy("payment_type").count().show()
print("Without cache:", time.time() - start)

# Cache
df.cache()
df.count()

# With cache
start = time.time()
df.groupBy("payment_type").count().show()
print("With cache:", time.time() - start)

# Partition test
df4 = df.repartition(4)
df8 = df.repartition(8)

start = time.time()
df4.count()
print("4 partitions:", time.time() - start)

start = time.time()
df8.count()
print("8 partitions:", time.time() - start)
