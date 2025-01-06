from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Resource Peaks and Eviction Correlation with Time") \
    .getOrCreate()

# Define the correct column names for task_usage and task_events
task_usage_columns = [
    "start_time", "end_time", "job_ID", "task_index", "machine_ID", 
    "CPU_rate", "canonical_memory_usage", "assigned_memory_usage", "unmapped_page_cache", 
    "total_page_cache", "maximum_memory_usage", "disk_IO_time", "local_disk_space_usage", 
    "maximum_CPU_rate", "maximum_disk_IO_time", "cycles_per_instruction", 
    "memory_accesses_per_instruction", "sample_portion", "aggregation_type", "sampled_CPU_usage"
]

task_events_columns = [
    "time", "missing_info", "job_ID", "task_index", "machine_ID", 
    "event_type", "user", "scheduling_class", "priority", 
    "CPU_request", "memory_request", "disk_space_request", 
    "different_machines_restriction"
]

# Load the task_usage dataset
task_usage_df = spark.read.csv("../data/task_usage-part-00000-of-00500.csv.gz", header=False, inferSchema=True)
task_usage_df = task_usage_df.toDF(*task_usage_columns)

# Load the task_events dataset
task_events_df = spark.read.csv("../data/task_events-part-00000-of-00500.csv.gz", header=False, inferSchema=True)
task_events_df = task_events_df.toDF(*task_events_columns)

# Define thresholds for high resource consumption (e.g., 90th percentile)
cpu_threshold = task_usage_df.approxQuantile("CPU_rate", [0.9], 0.01)[0]
memory_threshold = task_usage_df.approxQuantile("canonical_memory_usage", [0.9], 0.01)[0]

# Identify resource peaks (high CPU or memory usage)
resource_peaks_df = task_usage_df.filter(
    (col("CPU_rate") >= cpu_threshold) | (col("canonical_memory_usage") >= memory_threshold)
).select("machine_ID", "start_time", "end_time")

# Identify eviction events
evictions_df = task_events_df.filter(col("event_type") == 2).select("machine_ID", "time")

# Alias the DataFrames to disambiguate column references
evictions_df = evictions_df.alias("evictions")
resource_peaks_df = resource_peaks_df.alias("peaks")

# Join resource peaks with evictions on machine_ID and time overlap
time_overlap_df = evictions_df.join(
    resource_peaks_df,
    (col("evictions.machine_ID") == col("peaks.machine_ID")) &
    (col("evictions.time") >= col("peaks.start_time")) &
    (col("evictions.time") <= col("peaks.end_time")),
    "inner"
).select(col("evictions.machine_ID").alias("machine_ID"), col("evictions.time").alias("time"))

# Calculate the total number of evictions
total_evictions = evictions_df.count()

# Deduplicate the time_overlap_df by eviction time and machine_ID
unique_peak_evictions_df = time_overlap_df.distinct()

# Calculate the total number of unique evictions during resource peaks
peak_evictions = unique_peak_evictions_df.count()

# Calculate the percentage of evictions correlated with resource peaks
correlation_percentage = (peak_evictions / total_evictions) * 100

# Print the corrected results
print(f"Total eviction events: {total_evictions}")
print(f"Unique evictions during resource peaks: {peak_evictions}")
print(f"Percentage of evictions correlated with resource peaks: {correlation_percentage:.2f}%")

# Data for plotting
labels = ['Evictions During Peaks', 'Other Evictions']
sizes = [peak_evictions, total_evictions - peak_evictions]
colors = ['#FF5733', '#33A1FF']
explode = (0.1, 0)  # Slightly offset the first slice

import matplotlib.pyplot as plt

# Create the plot
plt.figure(figsize=(7, 7))
plt.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors, explode=explode, startangle=90)
plt.title("Evictions Correlated with Resource Peaks")
plt.tight_layout()

# Save the plot
plt.savefig("../plots/q7_evictions_resource_peaks.png")

# Show the plot
plt.show()

# Stop SparkSession
spark.stop()
