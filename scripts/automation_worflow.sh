#!/bin/bash

# Step 1: Navigate to the `data` directory
echo "Navigating to the 'data' directory..."
cd ../data

# Step 2: Check if the data file already exists
echo "Checking if data file already exists..."
if ls task_usage-part-*.csv.gz 1> /dev/null 2>&1; then
  echo "Data file already exists. Skipping download."
else
  echo "Data file not found. Running data download script..."
  # Run the data download logic
  ./download_task_usage.sh
fi

# Return to the original directory
cd - > /dev/null

# Step 2: Start the Kafka producer script
echo "Starting Kafka producer..."
python3 kafka_data_producer.py &
KAFKA_PID=$!
if [ $? -ne 0 ]; then
  echo "Error: Kafka producer script failed to start."
  exit 1
fi
echo "Kafka producer is running (PID: $KAFKA_PID)."

# Step 3: Start the Spark streaming script
echo "Starting Spark streaming analysis..."

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
  spark_streaming_cpu_analysis.py

if [ $? -ne 0 ]; then
  echo "Error: Spark streaming script failed."
  kill -9 $KAFKA_PID  # Stop the Kafka producer if Spark script fails
  exit 1
fi
kill -9 $KAFKA_PID
echo "Spark streaming analysis completed successfully."
