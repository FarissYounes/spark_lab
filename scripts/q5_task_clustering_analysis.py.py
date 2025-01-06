# Import necessary PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, approx_count_distinct

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Eviction Rate Analysis") \
    .getOrCreate()

# Define the correct column names for task_events
task_column_names = [
    "time", "missing_info", "job_ID", "task_index", "machine_ID", 
    "event_type", "user", "scheduling_class", "priority", 
    "CPU_request", "memory_request", "disk_space_request", 
    "different_machines_restriction"
]

# Load the task_events dataset
task_events_df = spark.read.csv("../data/task_events-part-00000-of-00500.csv.gz", header=False, inferSchema=True)

# Rename columns to the expected schema
task_events_df = task_events_df.toDF(*task_column_names)

# Group by job_ID and calculate the number of unique machine_IDs per job
job_machine_distribution = task_events_df.groupBy("job_ID") \
    .agg(
        count("task_index").alias("total_tasks"),
        approx_count_distinct("machine_ID").alias("unique_machines")
    )

# Calculate jobs where all tasks run on the same machine
same_machine_jobs = job_machine_distribution.filter(col("unique_machines") == 1).count()
total_jobs = job_machine_distribution.count()

# Calculate the percentage of jobs where all tasks run on the same machine
same_machine_percentage = (same_machine_jobs / total_jobs) * 100

print(f"Percentage of jobs where all tasks run on the same machine: {same_machine_percentage:.2f}%")

# Stop SparkSession
spark.stop()
