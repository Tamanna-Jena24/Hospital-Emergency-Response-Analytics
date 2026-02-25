from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# -----------------------------------------
# Create Spark Session
# -----------------------------------------
spark = SparkSession.builder \
    .appName("HospitalBatchProcessing") \
    .getOrCreate()

print("Hospital Batch Spark Started")

# -----------------------------------------
# Read Hospital CSV
# -----------------------------------------
df = spark.read.csv(
    "hospital_final.csv",
    header=True,
    inferSchema=True
)

print("Original Hospital Data")
df.show()

# -----------------------------------------
# Data Cleaning
# -----------------------------------------

# Convert arrival_time to timestamp
df = df.withColumn("arrival_time",
                   to_timestamp(col("arrival_time"),
                                "yyyy-MM-dd HH:mm:ss"))

# Remove duplicates
df = df.dropDuplicates()

# Remove null values
df = df.dropna()

# Filter only valid triage levels
df = df.filter(
    col("triage_level").isin("Critical", "High", "Medium", "Low")
)

print("Cleaned Hospital Data")
df.show()

# -----------------------------------------
# Write Output as Parquet
# -----------------------------------------
df.write.mode("overwrite") \
    .parquet("output/hospital_batch_cleaned")

print("Batch Hospital Data Processed Successfully!")

spark.stop()