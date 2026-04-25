from pyspark.sql import SparkSession
from pyspark.sql.functions import rank, col
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Q6 DF").getOrCreate()

df = spark.read.csv(
    "file:///home/reem/project/data/taxi_sample.csv",
    header=True,
    inferSchema=True
)

df_loc = df.groupBy("pickup_longitude", "pickup_latitude") \
           .count()

window_spec = Window.orderBy(col("count").desc())

df_q6 = df_loc.withColumn("rank", rank().over(window_spec))

df_q6.show(10)
df_q6.explain(True)
