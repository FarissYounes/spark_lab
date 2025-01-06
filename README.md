# Spark Lab: Data Manipulation with Apache Spark

Welcome to the **Spark Lab** repository! This project showcases the practical application of Apache Spark for data manipulation and processing, leveraging its distributed computing capabilities to handle large datasets efficiently.

## Purpose

The main objective of this repository is to experiment with and demonstrate various data manipulation techniques using Apache Spark. The project explores real-world scenarios and applies Spark's features to process and analyze structured and unstructured data, including resource usage metrics, machine events, and job/task scheduling information from distributed computing systems.

## Overview

This repository includes:
- **Code Examples**: Demonstrations of Spark's core functionalities.
- **Practical Use Cases**: Hands-on applications of Spark to process real-world datasets, focusing on performance optimization and scalability.
- **Dataset Integration**: Experiments utilizing the [Google Cluster-Usage Traces Dataset](https://github.com/google/cluster-data), which provides insights into scheduling and resource usage complexities in distributed systems.

## Dataset Description

The dataset contains traces collected from a Google Compute Cluster, capturing detailed information about the workload over a specific period. It is a valuable resource for studying distributed systems, task scheduling, and resource allocation strategies.

### Key Specifications:
The dataset provides comprehensive traces from a Google Compute Cluster, including details about machine capacities (CPU, memory) and lifecycle events. It captures job and task data such as submission, scheduling, completion, resource usage, constraints, and priorities. Resource utilization metrics, like CPU, memory, and disk usage, are recorded at 5-minute intervals. Timestamped events in microseconds enable precise activity analysis. The data is provided in compressed CSV format, with obfuscated sensitive fields (e.g., job names, user IDs) to ensure privacy while maintaining consistency for research and analysis.

### Use Case
This dataset is ideal for:
- Evaluating scheduling algorithms in distributed systems.
- Analyzing workload distribution and resource utilization patterns.
- Understanding the behavior of tasks under different constraints and priorities in large-scale compute clusters.

For detailed schema and examples, refer to the [official dataset documentation](https://github.com/google/cluster-data).

## Setup Instructions

Follow these steps to set up and run the project:

### 1. Data Extraction
Extract all the necessary data used in scripts files by running the `data_extraction.sh` script:
```bash
chmod +x data_extraction.sh
./data/data_extraction.sh

### 2. Create a Virtual Environment
Set up a Python virtual environment to isolate the dependencies:

```bash
python3 -m venv spark_env

Activate the virtual environment:
source spark_env/bin/activate

### 3. Install all the required Python packages from the requirements.txt file:
pip install -r requirements.txt
