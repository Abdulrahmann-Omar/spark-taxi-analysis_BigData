from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Q1 RDD").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

rdd = df.rdd

result = (rdd
    .map(lambda x: (x['tpep_pickup_datetime'], 1))
    .reduceByKey(lambda a, b: a + b))

print(result.take(10))
