from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("Partition Test").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

for p in [2, 4, 8]:
    df_p = df.repartition(p)

    start = time.time()
    df_p.count()
    end = time.time()

    print(f"{p} partitions time:", end - start)
