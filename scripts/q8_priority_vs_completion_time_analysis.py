from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, expr

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Task Priority vs Completion Time Analysis") \
    .getOrCreate()

# Define column names for task_events
task_columns = [
    "time", "missing_info", "job_ID", "task_index", "machine_ID",
    "event_type", "user", "scheduling_class", "priority",
    "CPU_request", "memory_request", "disk_space_request",
    "different_machines_restriction"
]

# Define column names for task_usage
usage_columns = [
    "start_time", "end_time", "job_ID", "task_index", "machine_ID", 
    "CPU_rate", "canonical_memory_usage", "assigned_memory_usage", "unmapped_page_cache", 
    "total_page_cache", "maximum_memory_usage", "disk_IO_time", "local_disk_space_usage", 
    "maximum_CPU_rate", "maximum_disk_IO_time", "cycles_per_instruction", 
    "memory_accesses_per_instruction", "sample_portion", "aggregation_type", "sampled_CPU_usage"
]

# Load the task_events dataset
task_events_df = spark.read.csv("../data/task_events-part-00000-of-00500.csv.gz", header=False, inferSchema=True)
task_events_df = task_events_df.toDF(*task_columns)

# Load the task_usage dataset
task_usage_df = spark.read.csv("../data/task_usage-part-00000-of-00500.csv.gz", header=False, inferSchema=True)
task_usage_df = task_usage_df.toDF(*usage_columns)

# Join task_events and task_usage on job_ID, task_index, and machine_ID
# Focus only on finished tasks (event_type = 4)
finished_tasks_df = task_events_df.filter(col("event_type") == 4).join(
    task_usage_df,
    (task_events_df["job_ID"] == task_usage_df["job_ID"]) &
    (task_events_df["task_index"] == task_usage_df["task_index"]) &
    (task_events_df["machine_ID"] == task_usage_df["machine_ID"]),
    "inner"
)

# Calculate task completion time (end_time - start_time)
completion_time_df = finished_tasks_df.withColumn(
    "completion_time", (col("end_time") - col("start_time"))
)

# Group by priority and calculate the average completion time
priority_vs_completion_time = completion_time_df.groupBy("priority") \
    .agg(avg("completion_time").alias("avg_completion_time")) \
    .orderBy("priority")

# Show the result
priority_vs_completion_time.show()

# Convert to Pandas for visualization
priority_vs_completion_time_pd = priority_vs_completion_time.toPandas()

# Plot the results
import matplotlib.pyplot as plt
plt.figure(figsize=(8, 6))
plt.bar(priority_vs_completion_time_pd["priority"], priority_vs_completion_time_pd["avg_completion_time"], color="skyblue")
plt.title("Task Priority vs Average Completion Time")
plt.xlabel("Task Priority")
plt.ylabel("Average Completion Time (ms)")
plt.tight_layout()

# Save and display the plot
plt.savefig("../plots/q8_priority_vs_completion_time.png")
plt.show()

# Stop SparkSession
spark.stop()
