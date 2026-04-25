from pyspark.sql import SparkSession
from pyspark.sql.functions import count, to_date

spark = SparkSession.builder.appName("Q1 DF").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

df_q1 = df.groupBy(to_date("tpep_pickup_datetime").alias("date")) \
          .agg(count("*").alias("total_trips"))

df_q1.show()
df_q1.explain(True)
