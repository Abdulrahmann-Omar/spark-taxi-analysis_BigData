from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("Caching Test").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

# Without cache
start = time.time()
df.count()
print("Without cache:", time.time() - start)

# Cache
df.cache()
df.count()

start = time.time()
df.count()
print("With cache:", time.time() - start)
