from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Q1 SQL").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

df.createOrReplaceTempView("taxi")

spark.sql("""
SELECT DATE(tpep_pickup_datetime) AS date, COUNT(*) AS total_trips
FROM taxi
GROUP BY DATE(tpep_pickup_datetime)
""").show()
