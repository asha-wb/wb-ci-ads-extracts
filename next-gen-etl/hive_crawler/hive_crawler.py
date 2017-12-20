#! /usr/bin/python

"""
A crawler to read S3 files, generate table schema, and import into Hive metastore.

Usage: spark-submit \
        --conf spark.hadoop.hadoop.security.credential.provider.path jceks://file/path/to/jceks \
        hive_crawler.py \
            --database [default] \
            --json_serde [hdfs://path/to/jars/hive-hcatalog-core.jar] \
            --job_id [jr_asdsfdsf] \
            s3://my-bucket/directory/to/scrape
"""

import boto3, datetime, logging, argparse, re, hashlib, random, jks
import parquet, json, csv
from chardet.universaldetector import UniversalDetector
from anytree import Node, RenderTree, PreOrderIter
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.utils import AnalysisException

# python 2 compatibility
try:
    from io import StringIO
except ImportError:
    import StringIO

class CrawlerTable(object):
    """ A table created by the crawler """
    def __init__(self, key, bucket='', name=None, partitioned=False, modified_time=None, files=[]):
        self.key = key
        self.bucket = bucket
        self.s3_location = 's3n://{}/{}'.format(bucket, key)
        self.s3_directory = self.s3_location.replace('s3n://', 's3a://')
        self.modified_time = modified_time
        self.partitioned = partitioned
        self.files = files

        if not name:
            name = self.s3_location.rsplit('/', 1)[1].replace('.', '_').replace(' ', '-')

        self.name = name
        self.logger = logging.getLogger(self.__class__.__name__)
        self.use_csv_header = True

        self.logger.debug('Created table %s from S3 location %s', self.name, self.s3_location)

        if self.files:
            self.logger.debug(self.files)
        else:
            self.s3_directory = self.s3_location.rsplit('/', 1)[0]

    def is_content_json(self, contents):
        """ Check if content is json """
        try:
            json.loads(contents.split("\n", 1)[0])
            return True
        except json.decoder.JSONDecodeError:
            self.logger.debug('Failed json type check')
            return False

    def is_content_parquet(self, contents):
        """ Check if content is parquet """
        try:
            str_io = StringIO(contents)
            for row in parquet.reader(str_io):
                return True
        except OSError:
            self.logger.debug('Failed parquet type check')
            return False

    def is_content_csv(self, contents):
        """ Check if string is CSV content. Sets use_csv_header to false if first row
            values are not valid column names. """
        try:
            str_io = StringIO(contents)
            column_names = []
            for row in csv.DictReader(str_io):
                for col in row.keys():
                    if col.lower() not in column_names and re.match('^[a-z0-9_-]+', col.lower()):
                        column_names.append(col.lower())
                    else:
                        self.use_csv_header = False
                return True
        except OSError:
            self.logger.debug('Failed csv type check')
            return False
        except csv.Error as e:
            self.logger.warn('Caught CSV error: %s', str(e))
            return True

    def build_dataframe(self, file_type, database, spark_session, hive_context, job_id):
        """ Build a dataframe and create external Hive table """
        self.logger.debug('Creating dataframe from %s format', file_type)
        try:
            if file_type == 'csv':
                df = spark_session.read.csv(self.s3_location, header=self.use_csv_header, inferSchema=True, mode='DROPMALFORMED')
                file_format = 'STORED AS TEXTFILE'
                row_format = "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
            elif file_type == 'json':
                df = spark_session.read.json(self.s3_location, mode='DROPMALFORMED')
                file_format = 'STORED AS TEXTFILE'
                row_format = "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'"
            elif file_type == 'parquet':
                df = spark_session.read.parquet(self.s3_location)
                file_format = 'STORED AS PARQUET'
                row_format = ''
        except AnalysisException as e:
            self.logger.error('Caught dataframe error: %s', str(e))
            return False

        # TODO: partitioning

        col_list = []
        for col in df.schema:
            json_col = col.jsonValue()
            col_list.append('{} {}'.format(json_col['name'], json_col['type'].upper()))

        drop_query = "DROP TABLE IF EXISTS {database}.{name}".format(database=database, name=self.name)
        create_query = """CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{name}
                            ({col_list}) 
                            {row_format} 
                            {file_format} 
                            LOCATION '{s3_location}'
                            TBLPROPERTIES('crawler.file.location'='{s3_location}',
                                          'crawler.file.modified_time'='{modified_time}',
                                          'crawler.file.num_rows'={num_rows},
                                          'crawler.file.num_files'={num_files},
                                          'crawler.file.partitioned'='{partitioned}',
                                          'crawler.file.file_type'='{file_type}',
                                          'crawler.job.id'='{job_id}')
                        """.format(
                            database=database,
                            name=self.name,
                            col_list=','.join(col_list),
                            row_format=row_format,
                            file_format=file_format,
                            s3_location=self.s3_directory,
                            modified_time=self.modified_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                            num_rows=df.count(),
                            num_files=max(1, len(self.files)),
                            partitioned=str(self.partitioned).lower(),
                            file_type=file_type,
                            job_id=job_id
                        )

        self.logger.debug(drop_query)
        hive_context.sql(drop_query)

        self.logger.debug(create_query)
        hive_context.sql(create_query)
        return True

    def create_hive_table(self, s3_client, database='default', spark_session=None, hive_context=None, job_id=''):
        """ Create a Hive table using generated schema """
        file = self.s3_location
        if self.files:
            file = self.files[0]

        key = file.replace('s3n://', '').split('/', 1)[1]

        self.logger.debug('Reading from s3://%s/%s', self.bucket, key)
        obj = s3_client.get_object(Bucket=self.bucket, Key=key)
        try:
            contents = obj['Body'].read()
            detector = UniversalDetector()

            for line in contents.split(b"\n"):
                detector.feed(line)
                if detector.done:
                    break

            detector.close()
            enc = detector.result
            self.logger.debug('Guessed file encoding %s', enc['encoding'])
            contents = contents.decode(enc['encoding'], errors='ignore')

            if self.is_content_json(contents):
                self.build_dataframe('json', database, spark_session, hive_context, job_id)
            elif self.is_content_parquet(contents):
                self.build_dataframe('parquet', database, spark_session, hive_context, job_id)
            elif self.is_content_csv(contents):
                self.build_dataframe('csv', database, spark_session, hive_context, job_id)
            else:
                raise Exception('Unable to safely identify content type')
        except UnicodeDecodeError as e:
            self.logger.error('Failed to decode file: %s', str(e))


    def repr(self):
        """ object represetation """
        return '<CrawlerTable({}) {}>'.format(self.key, self.name)

class Crawler(object):
    """ Crawler class. Scrapes files from S3 and creates logical tables from them. """
    def __init__(self, bucket, path, s3_client=None):
        self.bucket = bucket
        self.path = path
        self.s3 = s3_client
        self.root = Node('{}/{}'.format(self.bucket, self.path))

    def crawl(self):
        """ Crawls S3 location and generates a tree from the file structure """
        for file in self.s3.list_objects(Bucket=self.bucket, Prefix=self.path).get('Contents', []):
            if file['Key'][-1] != '/':
                current_root = self.root
                for d in file['Key'].replace(self.path, '').split('/'):
                    if d != '':
                        found = False
                        for child in current_root.children:
                            if child.name == d:
                                current_root = child
                                found = True

                        if not found:
                            node = Node(d, parent=current_root, scraped=False, partitioned=False, file_obj=file)
                            current_root = node

    def get_tables(self):
        """ Using the tree generated by crawl(), build CrawlerTable objects """
        tables = []
        for node in PreOrderIter(self.root):
            if node.is_leaf:
                if node.depth > 2:
                    node.partitioned = True

                if not node.path[1].scraped:
                    if len(node.siblings) == 0 or node.parent == self.root:
                        parent_path = ''
                        if node.parent != self.root:
                            parent_path = node.parent.name + '/'

                        table = CrawlerTable(
                            self.path + '/' + parent_path + node.name,
                            bucket=self.bucket,
                            partitioned=node.partitioned,
                            modified_time=node.file_obj['LastModified']
                        )
                    else:
                        files = ['s3n://' + '/'.join([p.name for p in node.path])]
                        max_modified_time = node.file_obj['LastModified']
                        for f in node.siblings:
                            files.append('s3n://' + '/'.join([p.name for p in f.path]))
                            if f.file_obj['LastModified'] > max_modified_time:
                                max_modified_time = f.file_obj['LastModified']

                        table = CrawlerTable(
                            self.path + '/' + node.path[1].name,
                            bucket=self.bucket,
                            partitioned=node.partitioned,
                            modified_time=max_modified_time,
                            files=files)

                    tables.append(table)
                    node.path[1].scraped = True
        return tables

    def print_tree(self):
        """ Print the generated tree """
        print(RenderTree(self.root))

def main():
    """ Main application logic """
    # Logging
    logging.basicConfig(format='[%(levelname)s] %(asctime)s %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    crawler_logger = logging.getLogger(CrawlerTable.__name__)
    crawler_logger.setLevel(logging.DEBUG)

    spark_context = SparkContext.getOrCreate()
    hive_context = HiveContext(spark_context)
    spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()

    # Command args
    parser = argparse.ArgumentParser('S3 metastore crawler')
    parser.add_argument('s3_dir', type=str, help='S3 directory to crawl')
    parser.add_argument('--database', type=str, default='default', help='Hive database')
    parser.add_argument('--json_serde', type=str, help='Path to JSONSerde jar')
    parser.add_argument('--job_id', type=str, help='Job ID to put in Hive table metadata')
    args = parser.parse_args()

    if args.s3_dir[0:2] != 's3' or '://' not in args.s3_dir:
        raise AttributeError('s3_dir must be in the format s3://<bucket>/<key>/')

    # This isn't ideal. Consider other forms of credential storage
    for conf in spark_context._conf.getAll():
        if conf[0] == 'spark.hadoop.hadoop.security.credential.provider.path':
            keystore = jks.KeyStore.load(conf[1].replace('jceks://file', ''), 'none')

    if not keystore:
        raise AttributeError('spark.hadoop.hadoop.security.credential.provider.path is required')

    # Hack to get json serde working. Shouldn't be necessary on hortonworks cluster
    if args.json_serde:
        hive_context.sql('ADD JAR {}'.format(args.json_serde))

    # Generate a job ID if one isn't provided
    if not args.job_id:
        args.job_id = 'jr_{}'.format(
            hashlib.sha256((datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.')+
                            str(random.randint(1, 1000))).encode()).hexdigest()
        )

    logger.info('Starting job %s', args.job_id)
    hive_context.sql('CREATE DATABASE IF NOT EXISTS {}'.format(args.database))

    no_urn = args.s3_dir.replace('s3://', '').replace('s3n://', '').replace('s3a://', '')
    bucket = no_urn.split('/', 1)[0]
    key = no_urn.split('/', 1)[1]

    aws_access_key_id = keystore.secret_keys['fs.s3a.access.key'].key.decode()
    aws_secret_access_key = keystore.secret_keys['fs.s3a.secret.key'].key.decode()
    session = boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='us-west-2')
    s3 = session.client(service_name='s3')

    # Create a file-system tree
    crawler = Crawler(bucket, key, s3_client=s3)
    crawler.crawl()
    crawler.print_tree()

    # Iterate over tree and create logical root-level tables
    tables = crawler.get_tables()

    # Import tables into Hive metastore
    for table in tables:
        table.create_hive_table(
            s3,
            database=args.database,
            spark_session=spark_session,
            hive_context=hive_context,
            job_id=args.job_id
        )

    # Show created tables
    hive_context.sql('USE {}'.format(args.database))
    tables_df = hive_context.sql('show tables')
    tables_df.show()

    # Show table properties
    for table in tables_df.collect():
        tblproperties = "show tblproperties {database}.{name}".format(
            database=args.database,
            name=table.tableName)

        details = hive_context.sql(tblproperties)
        details.show()

    logger.info('Finished job %s', args.job_id)

if __name__ == '__main__':
    main()
