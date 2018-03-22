#!/usr/bin/python

import hashlib
import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession, HiveContext

from hive_crawler import Crawler, CrawlerTable
from test_base import TestBaseClass

class TestCrawlerTableFunctions(TestBaseClass):
    def test_is_csv(self):
        table = CrawlerTable('test')
        contents = self.get_csv_string()

        assert table.is_content_csv(contents)
    
    def test_is_not_csv(self):
        table = CrawlerTable('test')
        contents = hashlib.sha256("not csv".encode()).hexdigest()

        assert not table.is_content_csv(contents)
    
    def test_is_json(self):
        table = CrawlerTable('test')
        contents = self.get_json_string()

        assert table.is_content_json(contents)

    def test_is_not_json(self):
        table = CrawlerTable('test')
        contents = hashlib.sha256("not json".encode()).hexdigest()

        assert not table.is_content_json(contents)
    
    def test_is_parquet(self):
        table = CrawlerTable('test')
        contents = self.get_parquet_string()
        if not contents:
            print('Skipping parquet test, no fastparquet library available')

        assert table.is_content_parquet(contents)

    def test_is_not_parquet(self):
        table = CrawlerTable('test')
        contents = hashlib.sha256("not parquet".encode()).hexdigest()

        assert not table.is_content_parquet(contents)

    def test_build_dataframe_csv(self):
        spark_context = SparkContext.getOrCreate()
        spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
        self.try_build_test_s3_directory(spark_context)

        table = CrawlerTable(
            self.test_dir + '/incoming/test_csv/test_csv.csv',
            bucket=self.s3_bucket,
            name='test_csv'
        )

        df = table.build_dataframe('csv', spark_session)
        assert df.count() == 2

    def test_build_dataframe_json(self):
        spark_context = SparkContext.getOrCreate()
        spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
        self.try_build_test_s3_directory(spark_context)

        table = CrawlerTable(
            self.test_dir + '/incoming/test_json/test_json.json',
            bucket=self.s3_bucket,
            name='test_json'
        )

        df = table.build_dataframe('json', spark_session)
        assert df.count() == 2

    def test_build_dataframe_parquet(self):
        spark_context = SparkContext.getOrCreate()
        spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
        self.try_build_test_s3_directory(spark_context)

        table = CrawlerTable(
            self.test_dir + '/incoming/test_parquet/test_parquet.parquet',
            bucket=self.s3_bucket,
            name='test_parquet'
        )

        df = table.build_dataframe('parquet', spark_session)
        assert df.count() == 2

    def test_create_hive_table(self):
        spark_context = SparkContext.getOrCreate()
        hive_context = HiveContext(spark_context)
        spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
        self.try_build_test_s3_directory(spark_context)

        table = CrawlerTable(
            self.test_dir + '/incoming/test_csv/test_csv.csv',
            bucket=self.s3_bucket,
            name='test_csv',
            modified_time=datetime.datetime.now()
        )

        table.create_hive_table(
            s3_client=self.s3,
            spark_session=spark_session,
            hive_context=hive_context,
            process_name='hive_crawler_unit_testing'
        )

        query = "SELECT COUNT(*) FROM default.test_csv"
        assert hive_context.sql(query).collect()[0][0] == 2

        drop = "DROP TABLE IF EXISTS default.test_csv"
        hive_context.sql(drop)

class TestCrawlerFunctions(TestBaseClass):
    def test_crawl(self):
        self.try_build_test_s3_directory()
        crawler = Crawler(bucket=self.s3_bucket, path=self.test_dir+'/incoming', s3_client=self.s3)
        crawler.crawl()

        assert len(crawler.root.children) == 3
    
    def test_get_tables(self):
        self.try_build_test_s3_directory()
        crawler = Crawler(bucket=self.s3_bucket, path=self.test_dir+'/incoming', s3_client=self.s3)
        crawler.crawl()

        tables = crawler.get_tables()

        assert len(tables) == 3
    
    def test_user_dir(self):
        self.try_build_test_s3_directory(user_dir='user')
        crawler = Crawler(bucket=self.s3_bucket, path=self.test_dir+'/incoming', s3_client=self.s3, user_dirs=True)
        crawler.crawl()
        tables = crawler.get_tables()

        for table in tables:
            assert table.user_dir

        for file in self.s3.list_objects(Bucket=self.s3_bucket, Prefix=self.test_dir+'/incoming/user/').get('Contents', []):
            self.s3.delete_object(Bucket=self.s3_bucket, Key=file['Key'])
