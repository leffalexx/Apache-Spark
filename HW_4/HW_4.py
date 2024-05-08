from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


spark = SparkSession.builder \
    .appName("TestStream") \
    .getOrCreate()

df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load()

df_with_odd_sum = df.withColumn("odd_sum", expr("aggregate(filter(sequence(0, value), x -> x % 2 != 0), 0, (acc, x) -> acc + x)"))

query = df_with_odd_sum.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
