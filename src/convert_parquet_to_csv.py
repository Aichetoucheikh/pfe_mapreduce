from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Convert").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark.read.parquet("/app/data/*.parquet")
print(f"  → {df.count():,} lignes")
df.coalesce(4).write.option("header","true").mode("overwrite").csv("/app/data/csv_output")
print("Done → /app/data/csv_output/")
spark.stop()
