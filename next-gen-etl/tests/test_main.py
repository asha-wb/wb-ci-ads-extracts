#!/usr/bin/python

import sys
import os
from mock import patch
from pyspark import SparkContext
from pyspark.sql import HiveContext

from hive_crawler import main as crawler_main
from s3_load import main as load_main
from test_base import TestBaseClass

class TestMainFunctions(TestBaseClass):
    def test_crawler_main(self):
        self.try_build_test_s3_directory()
        spark_context = SparkContext.getOrCreate()
        hive_context = HiveContext(spark_context)
        spark_context._conf.set('spark.hadoop.hadoop.security.credential.provider.path', os.environ['CREDENTIAL_FILE'])

        testargs = [
            'prog',
            '--process_name', 'hive_crawler_unit_testing',
            's3://{}/{}/incoming'.format(self.s3_bucket, self.test_dir)
        ]

        with patch.object(sys, 'argv', testargs):
            crawler_main()

        query = "SELECT COUNT(*) FROM default.test_json_json"
        assert hive_context.sql(query).collect()[0][0] == len(self.dummy_content)
    
    def test_s3_load_main(self):
        spark_context = SparkContext.getOrCreate()
        hive_context = HiveContext(spark_context)
        spark_context._conf.set('spark.hadoop.hadoop.security.credential.provider.path', os.environ['CREDENTIAL_FILE'])

        testargs = [
            'prog',
            '--process_name', 'hive_crawler_unit_testing',
            '--data_lake', 's3a://{}/{}/data-lake'.format(self.s3_bucket, self.test_dir),
            '--redshift_prefix', 'unit_testing_',
            '--redshift_temp_dir', 's3://{}/{}/redshift-temp'.format(self.s3_bucket, self.test_dir)
        ]

        with patch.object(sys, 'argv', testargs):
            load_main()
