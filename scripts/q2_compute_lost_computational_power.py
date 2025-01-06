# Import necessary PySpark and plotting modules
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, min as _min, sum as _sum
import matplotlib.pyplot as plt

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Computational Power Lost Due to Maintenance") \
    .getOrCreate()

# Define the correct column names based on the dataset schema
column_names = ["time", "machine_ID", "event_type", "platform_ID", "CPUs", "Memory"]

# Load dataset
df = spark.read.csv("../data/machine_events-part-00000-of-00001.csv.gz", header=False, inferSchema=True)

# Rename columns to the expected schema
df = df.toDF(*column_names)

# Identify the earliest ADD event for each machine (event_type = 0)
window_add = Window.partitionBy("machine_ID").orderBy(col("time").asc())
df_add = df.filter(df["event_type"] == 0).withColumn("rank", _min("time").over(window_add))
df_add = df_add.filter(col("rank") == col("time"))

# Calculate total CPU capacity from all added machines
total_cpu_capacity = df_add.agg(_sum("CPUs")).collect()[0][0]

# Identify the earliest REMOVE event for each machine (event_type = 1)
window_remove = Window.partitionBy("machine_ID").orderBy(col("time").asc())
df_remove = df.filter(df["event_type"] == 1).withColumn("rank", _min("time").over(window_remove))
df_remove = df_remove.filter(col("rank") == col("time"))

# Calculate total lost CPU capacity from removed machines
lost_cpu_capacity = df_remove.agg(_sum("CPUs")).collect()[0][0]

# Compute the percentage of computational power lost
percentage_lost = (lost_cpu_capacity / total_cpu_capacity) * 100
print(f"Percentage of computational power lost due to maintenance: {percentage_lost:.2f}%")

# Visualize the results as a pie chart
labels = ['Computation at 100% Availability', 'Lost CPU Capacity']
sizes = [total_cpu_capacity - lost_cpu_capacity, lost_cpu_capacity]
colors = ['#4CAF50', '#FF5722']

plt.figure(figsize=(7, 7))
plt.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90, explode=(0, 0.1))
plt.title("Percentage of Computational Power Lost Due to Maintenance")
plt.tight_layout()
plt.savefig("../plots/q2_computational_power_loss.png")  # Save the chart as a PNG file
plt.show()

# Stop SparkSession
spark.stop()
