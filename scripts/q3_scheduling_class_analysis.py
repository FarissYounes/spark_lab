# Import necessary PySpark and plotting modules
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Job and Task Events Analysis") \
    .getOrCreate()

# Define the correct column names for job_events
job_column_names = [
    "time", "missing_info", "job_ID", "event_type", "user", 
    "scheduling_class", "job_name", "logical_job_name"
]

# Define the correct column names for task_events
task_column_names = [
    "time", "missing_info", "job_ID", "task_index", "machine_ID", 
    "event_type", "user", "scheduling_class", "priority", 
    "CPU_request", "memory_request", "disk_space_request", 
    "different_machines_restriction"
]

# Load the job_events dataset
job_events_df = spark.read.csv("../data/job_events-part-00000-of-00500.csv.gz", header=False, inferSchema=True)

# Rename columns to the expected schema
job_events_df = job_events_df.toDF(*job_column_names)

# Load the task_events dataset
task_events_df = spark.read.csv("../data/task_events-part-00000-of-00500.csv.gz", header=False, inferSchema=True)

# Rename columns to the expected schema
task_events_df = task_events_df.toDF(*task_column_names)

# Distribution of jobs per scheduling class
job_distribution = job_events_df.groupBy("scheduling_class").count().orderBy("scheduling_class")

# Distribution of tasks per scheduling class
task_distribution = task_events_df.groupBy("scheduling_class").count().orderBy("scheduling_class")

# Show the distributions
print("Job Distribution by Scheduling Class:")
job_distribution.show()

print("Task Distribution by Scheduling Class:")
task_distribution.show()

# Convert to Pandas for visualization
job_distribution_pd = job_distribution.toPandas()
task_distribution_pd = task_distribution.toPandas()

# Plot the job distribution
job_distribution_pd.plot(kind="bar", x="scheduling_class", y="count", legend=False, 
                          title="Job Distribution by Scheduling Class")
plt.xlabel("Scheduling Class")
plt.ylabel("Number of Jobs")
plt.tight_layout()  # Adjust layout to prevent label overlap
plt.savefig("../plots/q3_job_distribution_scheduling_class.png")  # Save the chart as a PNG file
plt.show()

# Plot the task distribution
task_distribution_pd.plot(kind="bar", x="scheduling_class", y="count", legend=False, 
                           title="Task Distribution by Scheduling Class")
plt.xlabel("Scheduling Class")
plt.ylabel("Number of Tasks")
plt.tight_layout()  # Adjust layout to prevent label overlap
plt.savefig("../plots/q3_task_distribution_scheduling_class.png")  # Save the chart as a PNG file
plt.show()

# Stop SparkSession
spark.stop()
