#!/bin/sh
set -ei

export SPARK_MAJOR_VERSION=2

s3_dir=$1
database=$2
process_name=$3
credential_file=$4
spark_warehouse=$5

spark-submit \
    --master local \
    --driver-memory=8g \
    --executor-memory=3g \
    --conf spark.hadoop.hadoop.security.credential.provider.path="$credential_file" \
    --conf spark.hadoop.hive.metastore.warehouse.dir="$spark_warehouse" \
    --py-files=wb_next_gen_etl-1.0-py2.7.egg \
    run_crawler.py \
    --database "$database" \
    --process_name "$process_name" \
    "$s3_dir"
