#!/bin/sh
set -i

hdfs dfs -rm /user/btelle/etl_logs/crawler/*
hdfs dfs -mkdir /user/btelle/etl_logs/crawler/

tar xfz pylib.tar.gz

export SPARK_MAJOR_VERSION=2
export PYSPARK_PYTHON=`pwd`/etl/bin/python
export PYTHONPATH=`pwd`/etl/lib/python2.7/site-packages/

s3_dir=$1
database=$2
process_name=$3
credential_file=$4
spark_warehouse=$5

hdfs dfs -copyToLocal "$credential_file" credential_store.jceks
dir=`pwd`

spark-submit \
    --master local \
    --driver-memory=8g \
    --executor-memory=3g \
    --conf spark.hadoop.hadoop.security.credential.provider.path="jceks://file$dir/credential_store.jceks" \
    --conf spark.hadoop.hive.metastore.warehouse.dir="$spark_warehouse" \
    run_crawler.py \
    --database "$database" \
    --process_name "$process_name" \
    "$s3_dir" > /tmp/etl_run.log 2>&1

hdfs dfs -copyFromLocal /tmp/etl_run.log /user/btelle/etl_logs/crawler/crawler_run.log
