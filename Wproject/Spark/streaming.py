from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# -----------------------------------------
# Create Spark Session
# -----------------------------------------
spark = SparkSession.builder \
    .appName("HospitalStreamingProcessing") \
    .getOrCreate()

print("Hospital Streaming Started")

# -----------------------------------------
# Define Schema
# -----------------------------------------
schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("hospital_id", StringType(), True),
    StructField("arrival_time", StringType(), True),
    StructField("triage_level", StringType(), True),
    StructField("condition", StringType(), True)
])

# -----------------------------------------
# Read Streaming Data
# -----------------------------------------
stream_df = spark.readStream \
    .schema(schema) \
    .csv("streaming_input")

# Convert arrival_time
stream_df = stream_df.withColumn(
    "arrival_time",
    to_timestamp(col("arrival_time"),
                 "yyyy-MM-dd HH:mm:ss")
)

# Filter valid records
clean_stream = stream_df.filter(
    col("triage_level").isin("Critical", "High", "Medium", "Low")
)

# -----------------------------------------
# Write Streaming Output
# -----------------------------------------
query = clean_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output/hospital_stream") \
    .option("checkpointLocation", "output/checkpoint") \
    .start()

print("Streaming Running... Add files to streaming_input folder")

query.awaitTermination()