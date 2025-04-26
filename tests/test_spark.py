from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestApp").getOrCreate()
df = spark.createDataFrame([(1, "🔥"), (2, "🚀")], ["id", "emoji"])
df.show()
spark.stop()
