#!/bin/sh
set -ei

export SPARK_MAJOR_VERSION=2

database=$1
process_name=$2
s3_data_lake=$3
s3_redshift_temp=$4
s3_archive_dir=$5
credential_file=$6
spark_warehouse=$7
jdbcRedshiftDriver=$8
redshiftSchema=$9

spark-submit \
    --master local \
    --driver-memory=8g \
    --executor-memory=3g \
    --conf spark.hadoop.hadoop.security.credential.provider.path="$credential_file" \
    --conf spark.hadoop.hive.metastore.warehouse.dir="$spark_warehouse" \
    --packages com.databricks:spark-avro_2.11:3.2.0,com.databricks:spark-redshift_2.11:2.0.1
    --driver-class-path "$jdbcRedshiftDriver" \
    --py-files=wb_next_gen_etl-1.0-py2.7.egg \
    bin/run_load.py \
    --database "$database" \
    --process_name "$process_name" \
    --data_lake "$s3_data_lake" \
    --redshift_temp_dir "$s3_redshift_temp" \
    --redshift_schema "$redshiftSchema" \
    --clean_up \
    --archive_dir "$s3_archive_dir"
