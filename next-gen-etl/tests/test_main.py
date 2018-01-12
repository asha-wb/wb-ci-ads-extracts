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
        # parser.add_argument('s3_dir', type=str, help='S3 directory to crawl')
        # parser.add_argument('--database', type=str, default='default', help='Hive database')
        # parser.add_argument('--json_serde', type=str, help='Path to JSONSerde jar')
        # parser.add_argument('--process_name', type=str, help='Process name for logging', required=True)
        # parser.add_argument('--allow_single_files', default=False, action='store_true',
        #                    help='Allow single files in the base directory')
        # parser.add_argument('--external_tables', default=False, action='store_true',
        #                    help='Store generated tables as external tables in Hive')

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

        query = "SELECT COUNT(*) FROM default.test_csv_csv"
        assert hive_context.sql(query).collect()[0][0] == 2
    
    def test_s3_load_main(self):
        spark_context = SparkContext.getOrCreate()
        hive_context = HiveContext(spark_context)
        spark_context._conf.set('spark.hadoop.hadoop.security.credential.provider.path', os.environ['CREDENTIAL_FILE'])

        # parser.add_argument('--data_lake', type=str, help='Data lake location', required=True)
        # parser.add_argument('--redshift_schema', type=str, default='sandbox', help='Schema to put Redshift tables in')
        # parser.add_argument('--redshift_prefix', type=str, default='', help='Prefix for Redshift tables')
        # parser.add_argument('--redshift_temp_dir', type=str, help='Temp dir to use when loading Redshift tables', required=True)
        # parser.add_argument('--json_serde', type=str, help='Path to JSONSerde jar')

        testargs = [
            'prog',
            '--process_name', 'hive_crawler_unit_testing',
            '--data_lake', 's3a://{}/{}/data-lake'.format(self.s3_bucket, self.test_dir),
            '--redshift_prefix', 'unit_testing_',
            '--redshift_temp_dir', 's3://{}/{}/redshift-temp'.format(self.s3_bucket, self.test_dir)
        ]

        with patch.object(sys, 'argv', testargs):
            load_main()
