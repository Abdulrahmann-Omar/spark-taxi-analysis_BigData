from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, to_date

spark = SparkSession.builder.appName("Q3 DF").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

df_q3 = df.groupBy(to_date("tpep_pickup_datetime").alias("date")) \
          .agg(avg("fare_amount").alias("avg_fare"))

df_q3.show()
df_q3.explain(True)
