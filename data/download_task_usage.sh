#!/bin/bash

# Extract and download the first file from `task_usage`
FILES=$(gsutil ls gs://clusterdata-2011-2/task_usage/part-?????-of-?????.csv.gz | head -n 3)

# Loop through each file and download it
for FILE in $FILES; do
  echo "Downloading $FILE..."
  gsutil cp "$FILE" "./task_usage-$(basename "$FILE")"
done

echo "All files downloaded successfully."