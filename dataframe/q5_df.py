from pyspark.sql import SparkSession
from pyspark.sql.functions import count, to_date, avg
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Q5 DF").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

# Step 1: trips per day
df_daily = df.groupBy(to_date("tpep_pickup_datetime").alias("date")) \
             .agg(count("*").alias("total_trips"))

# Step 2: window
window_spec = Window.orderBy("date").rowsBetween(-2, 0)

df_q5 = df_daily.withColumn(
    "moving_avg",
    avg("total_trips").over(window_spec)
)

df_q5.show()
df_q5.explain(True)
