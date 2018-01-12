#!/bin/sh

set -ei

folder="oozie"
OOZIE_URL="http://nn1.bade.bane.cmdt.wb.com:11000/oozie/"
upload_dir="/user/btelle/next_gen_etl"

# export $OOZIE_URL

echo "Updating $folder"

# Validate workflows
oozie validate -oozie $OOZIE_URL "$folder/workflow.xml"

# Delete old files
HADOOP_USER_NAME=hdfs hadoop fs -mkdir -p "$upload_dir/"
HADOOP_USER_NAME=hdfs hadoop fs -rm -r -f -skipTrash "$upload_dir/$folder"

# Copy to HDFS
HADOOP_USER_NAME=hdfs hadoop fs -copyFromLocal -f "$folder" "$upload_dir/"

# Run job
oozie job -oozie $OOZIE_URL -config "$folder/job.properties" -run
