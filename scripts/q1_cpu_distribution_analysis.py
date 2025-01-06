# Import necessary PySpark and plotting modules
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MachineEventsAnalysis") \
    .getOrCreate()

# Define the correct column names based on the dataset schema
column_names = ["time", "machine_ID", "event_type", "platform_ID", "CPUs", "Memory"]

# Load dataset
df = spark.read.csv("../data/machine_events-part-00000-of-00001.csv.gz", header=False, inferSchema=True)

# Rename columns to the expected schema
df = df.toDF(*column_names)

# Distribution of machines according to their CPU capacity
# Group by CPU capacity and count
cpu_distribution = df.groupBy("CPUs").count().orderBy("CPUs")

# Show the CPU distribution result
cpu_distribution.show()

# Convert the results to a Pandas DataFrame for visualization
cpu_distribution_pd = cpu_distribution.toPandas()

# Plot the CPU distribution
cpu_distribution_pd.plot(kind="bar", x="CPUs", y="count", legend=False, title="Machine Distribution by CPU Capacity")
plt.xlabel("CPU Capacity")
plt.ylabel("Number of Machines")
plt.tight_layout()  # Adjust layout to prevent label overlap
plt.savefig("../plots/q1_cpu_distribution.png")  # Save the chart as a PNG file
plt.show()

# Stop SparkSession
spark.stop()
