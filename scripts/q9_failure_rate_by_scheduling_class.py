from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Failure Rate by Scheduling Class Analysis") \
    .getOrCreate()

# Define column names for task_events
task_columns = [
    "time", "missing_info", "job_ID", "task_index", "machine_ID",
    "event_type", "user", "scheduling_class", "priority",
    "CPU_request", "memory_request", "disk_space_request",
    "different_machines_restriction"
]

# Load the task_events dataset
task_events_df = spark.read.csv("../data/task_events-part-00000-of-00500.csv.gz", header=False, inferSchema=True)
task_events_df = task_events_df.toDF(*task_columns)

# Filter failed tasks (event_type = 3)
failed_tasks_df = task_events_df.filter(col("event_type") == 3)

# Count total tasks and failed tasks per scheduling class
total_tasks_by_class = task_events_df.groupBy("scheduling_class").count().withColumnRenamed("count", "total_tasks")
failed_tasks_by_class = failed_tasks_df.groupBy("scheduling_class").count().withColumnRenamed("count", "failed_tasks")

# Join the two DataFrames to compute the failure rate
failure_rate_df = total_tasks_by_class.join(failed_tasks_by_class, "scheduling_class", "left").fillna(0, subset=["failed_tasks"])

# Calculate the failure rate as a percentage
failure_rate_df = failure_rate_df.withColumn(
    "failure_rate",
    (col("failed_tasks") / col("total_tasks")) * 100
)

# Show the results
failure_rate_df.select("scheduling_class", "total_tasks", "failed_tasks", "failure_rate").show()

# Convert to Pandas for visualization
failure_rate_pd = failure_rate_df.toPandas()

# Plot the results
import matplotlib.pyplot as plt
plt.figure(figsize=(8, 6))
plt.bar(failure_rate_pd["scheduling_class"], failure_rate_pd["failure_rate"], color="salmon")
plt.title("Failure Rate by Scheduling Class")
plt.xlabel("Scheduling Class")
plt.ylabel("Failure Rate (%)")

# Set x-axis ticks explicitly to match scheduling classes
plt.xticks([0, 1, 2, 3])

plt.tight_layout()

# Save and display the plot
plt.savefig("../plots/q9_failure_rate_by_scheduling_class.png")
plt.show()

# Stop SparkSession
spark.stop()
