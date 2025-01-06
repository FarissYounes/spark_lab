#!/bin/bash

# Script to extract data from Google Cloud Storage and rename files with specific prefixes.

# Extract and download the first file from `job_events`
FIRST_FILE=$(gsutil ls gs://clusterdata-2011-2/job_events/part-?????-of-?????.csv.gz | head -n 1)
gsutil cp $FIRST_FILE ./job_events-$(basename $FIRST_FILE)

# Extract and download the first file from `task_events`
FIRST_FILE=$(gsutil ls gs://clusterdata-2011-2/task_events/part-?????-of-?????.csv.gz | head -n 1)
gsutil cp $FIRST_FILE ./task_events-$(basename $FIRST_FILE)

# Extract and download the first file from `machine_events`
FIRST_FILE=$(gsutil ls gs://clusterdata-2011-2/machine_events/part-?????-of-?????.csv.gz | head -n 1)
gsutil cp $FIRST_FILE ./machine_events-$(basename $FIRST_FILE)

# Extract and download the first file from `machine_events`
FIRST_FILE=$(gsutil ls gs://clusterdata-2011-2/task_usage/part-?????-of-?????.csv.gz | head -n 1)
gsutil cp $FIRST_FILE ./task_usage-$(basename $FIRST_FILE)

echo "Data extraction complete!"