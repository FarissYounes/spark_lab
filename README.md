# Spark Lab: Real-Time Data Processing with Apache Spark and Kafka

Welcome to the extended Spark Lab repository! This project showcases a complete real-time data processing pipeline using Apache Spark and Kafka. It highlights the integration of batch and streaming workflows to handle real-time data ingestion, transformation, and analysis.

## Purpose
The main objective of this feature is to create a seamless automated pipeline that downloads the dataset, streams it via Kafka, and processes it using Apache Spark's streaming capabilities. It simulates real-time workloads for distributed computing systems, providing insights into resource usage and system behavior.

## Overview

This repository includes:
- **Automated Workflow**: A single script to handle data download, Kafka producer setup, and Spark streaming execution.
- **Real-Time Data Streaming**: Simulates real-time data ingestion using Kafka.
- **Data Analysis**: Processes and analyzes streaming data using Spark Structured Streaming.

## Setup Instructions

Follow these steps to set up and run the project:

### 1. Data Extraction
Extract all the necessary data used in scripts files by running the `download_task_usage.sh` script:
```bash
  chmod +x ./data/download_task_usage.sh
  ./data/download_task_usage.sh
```
- But there is no need to run this sh file because it had been automated in the automation_worflow.sh file :
```bash
  # Step 1: Navigate to the `data` directory
  echo "Navigating to the 'data' directory..."
  cd ../data
  
  # Step 2: Check if the data file already exists
  echo "Checking if data file already exists..."
  if ls task_usage-part-*.csv.gz 1> /dev/null 2>&1; then
    echo "Data file already exists. Skipping download."
  else
    echo "Data file not found. Running data download script..."
    # Run the data download logic
    ./download_task_usage.sh
  fi
```

### 2. Create a Virtual Environment
Set up a Python virtual environment to isolate the dependencies:

```bash
  python3 -m venv spark_env
  
  # Activate the virtual environment:
  source spark_env/bin/activate
```

### 3. Install all the required Python packages from the requirements.txt file:
```bash
  pip install -r requirements.txt
```

### 4. Launching Kafka and Zookeeper Services :
```bash
  bin/kafka-server-start.sh config/server.properties
  bin/zookeeper-server-start.sh config/zookeeper.properties
```

## Automated Workflow Description
The automated workflow, encapsulated in the scripts/automation_workflow.sh script, performs the following steps:

- **Data Download**: Checks if the dataset is already downloaded. If not, it triggers the download process.
- **Kafka Producer**: Streams dataset records into a Kafka topic, appending timestamps to simulate real-time data generation.
- **Spark Streaming**: Processes the streamed data using Spark, filtering and aggregating CPU usage metrics.

  How to Run the Workflow
  Run the following command to execute the entire workflow:
  ```bash
    chmod +x ./scripts/automation_workflow.sh
    ./scripts/automation_workflow.sh
  ```

## View Pre-Executed Scripts 
If you do not want to set up the project locally, you can directly view the Jupyter Notebook, ./scripts/combined_scripts_exec.ipynb
which contains all the executed scripts and their results.
