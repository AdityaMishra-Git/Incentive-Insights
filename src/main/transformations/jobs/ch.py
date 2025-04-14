# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.appName("TestParquet").getOrCreate()
#
# # Creating a simple DataFrame
# test_df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
#
# # Attempt to write Parquet
# try:
#     test_df.write.format("parquet").mode("overwrite").save("test_parquet_output")
#     print("✅ Parquet write successful!")
# except Exception as e:
#     print(f"❌ Parquet write failed: {str(e)}")
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestParquet") \
    .master("local[*]") \
    .config("spark.executorEnv.PYTHONPATH", os.environ.get("PYSPARK_PYTHON")) \
    .getOrCreate()

print("✅ Spark started successfully!")
