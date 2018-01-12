#!/usr/bin/python

import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession, HiveContext

from s3_load import HiveTable, read_tables_from_hive
from utils import get_redshift_database_connection
from test_base import TestBaseClass

class TestReadTables(TestBaseClass):
    def test_read_tables(self):
        spark_context = SparkContext.getOrCreate()
        hive_context = HiveContext(spark_context)

        tables = read_tables_from_hive(hive_context, 'default')
        assert len(tables) == 3

class TestHiveTableFunctions(TestBaseClass):
    def test_properties(self):
        key = 'test_property'
        val = 'abc123'

        table = HiveTable('test_csv_csv', 'default')
        table.add_property(key, val)

        assert table.get_property(key) == val

    def test_build_dataframe(self):
        table = HiveTable('test_csv_csv', 'default')
        spark_context = SparkContext.getOrCreate()
        hive_context = HiveContext(spark_context)

        table.build_dataframe(hive_context)
        assert table.df
        assert table.df.count() == 2
    
    def test_meta_columns(self):
        spark_context = SparkContext.getOrCreate()
        hive_context = HiveContext(spark_context)
        self.try_build_test_s3_directory(spark_context)

        table = HiveTable('test_csv_csv', 'default')
        table.add_property('crawler.file.location', 's3n://{}/{}'.format(self.s3_bucket, self.test_dir))
        table.build_dataframe(hive_context)
        table.add_meta_columns(self.s3)

        assert table.pre_count == table.post_count
        assert table.df.select('metadata_file_owner').collect()[0] != ''
    
    def test_output_to_data_lake(self):
        spark_context = SparkContext.getOrCreate()
        hive_context = HiveContext(spark_context)
        self.try_build_test_s3_directory(spark_context)

        data_lake_dir = self.test_dir + '/data-lake'

        table = HiveTable('test_csv_csv', 'default')
        table.add_property('crawler.file.location', 's3n://{}/{}'.format(self.s3_bucket, self.test_dir))
        table.add_property('crawler.file.file_type', 'csv')
        table.add_property('crawler.file.num_rows', 2)

        table.build_dataframe(hive_context)
        table.add_meta_columns(self.s3)
        table.output_to_data_lake('s3a://{}/{}'.format(self.s3_bucket, data_lake_dir))

        files = self.s3.list_objects(Bucket=self.s3_bucket, Prefix=data_lake_dir)['Contents']

        assert files
        self.s3.delete_objects(Bucket=self.s3_bucket, Delete={'Objects': [{'Key': r['Key']} for r in files]})

    def test_output_to_redshift(self):
        spark_context = SparkContext.getOrCreate()
        hive_context = HiveContext(spark_context)
        self.try_build_test_s3_directory(spark_context)
        credentials = self.get_credentials()

        return True

        table = HiveTable('test_csv_csv', 'default')
        table.add_property('crawler.file.location', 's3n://{}/{}'.format(self.s3_bucket, self.test_dir))
        table.add_property('crawler.file.file_type', 'csv')
        table.add_property('crawler.file.num_rows', 2)

        table.build_dataframe(hive_context)
        table.add_meta_columns(self.s3)
        table.output_to_redshift(
            credentials['aws.redshift.host'],
            credentials['aws.redshift.user'],
            credentials['aws.redshift.password'],
            credentials['aws.redshift.database'],
            schema='sandbox',
            table_prefix='unit_testing_',
            temp_dir='s3a://{}/{}/redshift-temp'.format(self.s3_bucket, self.test_dir),
            spark_context=spark_context
        )

        conn = get_redshift_database_connection(
            credentials['aws.redshift.host'],
            credentials['aws.redshift.user'],
            credentials['aws.redshift.password'],
            credentials['aws.redshift.database']
        )
        cursor = conn.cursor()

        query = "SELECT COUNT(*) FROM sandbox.unit_testing_test_csv_csv;"
        cursor.execute(query)
        response = cursor.fetchone()

        assert response[0] == 2

        drop = "DROP TABLE IF EXISTS sandbox.unit_testing_test_csv_csv;"
        cursor.execute(drop)
        cursor.close()
        conn.close()
    
    def test_clean_up(self):
        spark_context = SparkContext.getOrCreate()
        hive_context = HiveContext(spark_context)
        self.try_build_test_s3_directory(spark_context)

        for table in read_tables_from_hive(hive_context, 'default'):
            table.build_dataframe(hive_context)
            table.add_meta_columns(self.s3)

            table.drop_table(hive_context)
            table.archive_files(
                's3a://{}/{}/archive'.format(self.s3_bucket, self.test_dir),
                s3_client=self.s3
            )

        tables = read_tables_from_hive(hive_context, 'default')
        assert not tables

        files = self.s3.list_objects(Bucket=self.s3_bucket, Prefix='{}/incoming'.format(self.test_dir))
        assert 'Contents' not in files
