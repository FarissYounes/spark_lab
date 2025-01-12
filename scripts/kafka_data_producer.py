from kafka import KafkaProducer
import time
from datetime import datetime
import gzip
import os

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Path to the dataset directory
data_dir = '../data'

try:
    # Iterate through all matching files in the directory
    for file_name in os.listdir(data_dir):
        if file_name.startswith('task_usage') and file_name.endswith('.csv.gz'):
            file_path = os.path.join(data_dir, file_name)
            print(f"Processing file: {file_path}")

            # Open and read the gzipped file
            with gzip.open(file_path, 'rt') as file:
                for line in file:
                    # Get the current timestamp in ISO format
                    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Strip any newline or extra spaces from the line
                    line = line.strip()
                    
                    # Append the timestamp to the record
                    message = f"{current_timestamp},{line}"
                    
                    # Send each line as an event to Kafka
                    producer.send('task-usage-data', value=message.encode('utf-8'))
                    time.sleep(0.01)  # Simulate delay between events

    print("All files have been processed and sent to Kafka.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Ensure the producer is closed on exit
    producer.close()
    print("Kafka producer has been closed.")
