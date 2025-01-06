# Import necessary PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Resource Request vs Consumption Analysis") \
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

# Rename columns to the expected schema
task_usage_df = task_usage_df.toDF(*task_usage_columns)

# Load the task_events dataset
task_events_df = spark.read.csv("../data/task_events-part-00000-of-00500.csv.gz", header=False, inferSchema=True)

# Rename columns to the expected schema
task_events_df = task_events_df.toDF(*task_events_columns)

# Join task_events and task_usage datasets on job_ID and task_index
combined_df = task_events_df.join(task_usage_df, ["job_ID", "task_index"], "inner")

# Select relevant columns for analysis
resource_comparison_df = combined_df.select(
    col("CPU_request"),
    col("memory_request"),
    col("CPU_rate"),
    col("canonical_memory_usage")
)

# Calculate correlation between requested and consumed resources
correlation_results = {
    "CPU_correlation": resource_comparison_df.stat.corr("CPU_request", "CPU_rate"),
    "Memory_correlation": resource_comparison_df.stat.corr("memory_request", "canonical_memory_usage")
}

# Print correlation results
print("Correlation between CPU request and CPU consumption:", correlation_results["CPU_correlation"])
print("Correlation between Memory request and Memory consumption:", correlation_results["Memory_correlation"])

# Stop SparkSession
spark.stop()
