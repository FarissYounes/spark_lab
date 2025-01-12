from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, current_timestamp, from_unixtime, unix_timestamp, expr, sum as _sum

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaStructuredStreamingSumCPU") \
    .getOrCreate()

# Read streaming data from Kafka
kafka_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "task-usage-data") \
    .load()

# Parse the incoming data
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as event_data")

# Extract relevant fields and cast start_time and CPU_rate
usage_stream = parsed_stream.selectExpr(
    "split(event_data, ',')[0] as start_time",                   # Extract start_time
    "CAST(split(event_data, ',')[6] AS FLOAT) as CPU_rate"       # Extract CPU_rate
)

# Convert start_time to Unix timestamp
usage_stream_with_time = usage_stream.withColumn(
    "start_time_unix",
    unix_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss")  # Parse start_time string
)

# Current Unix timestamp
current_unix = unix_timestamp(current_timestamp())
last_20_seconds_unix = current_unix - expr("20")

# Filter rows within the last 10 seconds
filtered_stream = usage_stream_with_time.filter(
    (col("start_time_unix") >= last_20_seconds_unix) &
    (col("start_time_unix") <= current_unix)
)

# Output only relevant columns for debugging
debug_query = filtered_stream.select("start_time_unix", "CPU_rate").writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

# Step 4: Calculate the sum of CPU_rate for the filtered rows
aggregated_stream = filtered_stream.groupBy().agg(
    _sum("CPU_rate").alias("total_CPU_rate")  # Sum of CPU_rate
)

# Debugging output for aggregated data
aggregated_query = aggregated_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

debug_query.awaitTermination()
aggregated_query.awaitTermination()