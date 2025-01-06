# Import necessary PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

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

# Count total tasks per scheduling class
total_tasks = task_events_df.groupBy("scheduling_class").count().withColumnRenamed("count", "total_tasks")

# Filter evicted tasks (event_type = 2) and group by scheduling_class, considering unique tasks
evicted_tasks = task_events_df.filter(col("event_type") == 2) \
    .groupBy("scheduling_class", "job_ID", "task_index") \
    .count() \
    .groupBy("scheduling_class") \
    .count().withColumnRenamed("count", "evicted_tasks")

# Join the total tasks and evicted tasks datasets
eviction_rate_df = total_tasks.join(evicted_tasks, "scheduling_class", "left") \
    .fillna(0, subset=["evicted_tasks"])  # Fill missing evicted tasks with 0

# Calculate the eviction rate
eviction_rate_df = eviction_rate_df.withColumn(
    "eviction_rate",
    (col("evicted_tasks") / col("total_tasks")) * 100
)

# Show the eviction rate for each scheduling class
eviction_rate_df.select("scheduling_class", "total_tasks", "evicted_tasks", "eviction_rate").show()

# Convert to Pandas for visualization
eviction_rate_pd = eviction_rate_df.select("scheduling_class", "eviction_rate").toPandas()

# Plot the eviction rate
import matplotlib.pyplot as plt

eviction_rate_pd = eviction_rate_pd.sort_values(by="scheduling_class")

eviction_rate_pd.plot(kind="bar", x="scheduling_class", y="eviction_rate", legend=False, 
                       title="Eviction Rate by Scheduling Class")
plt.xlabel("Scheduling Class")
plt.ylabel("Eviction Rate (%)")
plt.tight_layout()  # Adjust layout to prevent label overlap
plt.savefig("../plots/q4_eviction_rate_by_scheduling_class.png")  # Save the chart as a PNG file
plt.show()

# Stop SparkSession
spark.stop()
