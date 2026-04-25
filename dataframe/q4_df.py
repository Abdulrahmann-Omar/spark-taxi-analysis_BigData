from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Q4 DF").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

df_q4 = df.groupBy("passenger_count", "payment_type") \
          .count()

df_q4.show()
df_q4.explain(True)
