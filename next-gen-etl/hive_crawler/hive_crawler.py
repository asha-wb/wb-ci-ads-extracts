#! /usr/bin/python

"""
A crawler to read S3 files, generate table schema, and import into Hive metastore.

Usage: spark-submit \
        --conf spark.hadoop.hadoop.security.credential.provider.path jceks://file/path/to/jceks \
        hive_crawler.py \
            [ --database default ] \
            [ --json_serde hdfs://path/to/jars/hive-hcatalog-core.jar ] \
            [ --job_id jr_asdsfdsf ] \
            [ --allow_single_files ] \
            s3://my-bucket/directory/to/scrape
"""

import boto3, datetime, logging, argparse, re, hashlib, random, jks, time, random
import parquet, json, csv
from chardet.universaldetector import UniversalDetector
from anytree import Node, RenderTree, PreOrderIter
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import input_file_name
from utils import get_redshift_database_connection, update_etlprocess_dtls, get_etlprocess_dtls

# python 2 compatibility
try:
    from io import StringIO
except ImportError:
    import StringIO

class CrawlerTable(object):
    """ A table created by the crawler """
    def __init__(self, key, bucket='', name=None, partitioned=False, modified_time=None, files=[], is_base_file=False, crawler_base='', user_dir=False):
        self.key = key
        self.bucket = bucket
        self.s3_location = 's3n://{}/{}'.format(bucket, key)
        self.s3_directory = self.s3_location
        self.modified_time = modified_time
        self.partitioned = partitioned
        self.files = files
        self.is_base_file = is_base_file
        self.crawler_base = crawler_base

        if not name:
            name = self.s3_location.rsplit('/', 1)[1].replace('.', '_').replace(' ', '-')

        self.name = name
        self.logger = logging.getLogger(self.__class__.__name__)
        self.use_csv_header = True
        self.user_dir = user_dir

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
        except ValueError: # json in Python 2 raises a ValueError instead of JSONDecodeError
            self.logger.debug('Failed json type check')
            return False

    def is_content_parquet(self, contents):
        """ Check if content is parquet """
        if contents[0:3] == b'PAR':
            return True
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
        except UnicodeEncodeError as e:
            self.logger.debug('Caught Unicode error (%s), cannot continue', str(e))
        except csv.Error as e:
            self.logger.warn('Caught CSV error: %s', str(e))
            return True

    def build_dataframe(self, file_type, spark_session):
        """ Build a dataframe and create external Hive table """
        self.logger.debug('Creating dataframe from %s format', file_type)

        try:
            if file_type == 'csv':
                df = spark_session.read.csv(self.s3_location, header=self.use_csv_header, inferSchema=True, mode='DROPMALFORMED')
            elif file_type == 'json':
                df = spark_session.read.json(self.s3_location, mode='DROPMALFORMED')
            elif file_type == 'parquet':
                df = spark_session.read.parquet(self.s3_location)
        except AnalysisException as e:
            self.logger.error('Caught dataframe error: %s', str(e))
            return False
        
        return df

    def create_hive_table(self, s3_client, database='default', spark_session=None, hive_context=None, job_id='', process_name='', external=False):
        """ Create a Hive table using generated schema """
        file = self.s3_location
        if self.files:
            file = self.files[0]

        key = file.replace('s3n://', '').split('/', 1)[1]

        self.logger.debug('Reading from s3://%s/%s', self.bucket, key)
        obj = s3_client.get_object(Bucket=self.bucket, Key=key)
        try:
            df = False
            contents = obj['Body'].read()
            detector = UniversalDetector()

            for line in contents.split(b"\n"):
                detector.feed(line)
                if detector.done:
                    break

            detector.close()
            enc = detector.result

            if not enc['encoding']:
                enc['encoding'] = 'utf-8'

            self.logger.debug('Guessed file encoding %s', enc['encoding'])
            contents_decoded = contents.decode(enc['encoding'], errors='ignore')

            if self.is_content_json(contents_decoded):
                file_type = 'json'
                external_format = "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'"
                df = self.build_dataframe('json', spark_session)
            elif self.is_content_parquet(contents):
                file_type = 'parquet'
                external_format = 'STORED AS PARQUET'
                df = self.build_dataframe('parquet', spark_session)
            elif self.is_content_csv(contents_decoded):
                file_type = 'csv'
                external_format = "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
                df = self.build_dataframe('csv', spark_session)
            else:
                self.logger.info('Unable to safely identify content type')

        except UnicodeDecodeError as e:
            self.logger.error('Failed to decode file: %s', str(e))

        # TODO: partitioning

        if df == False:
            return False

        df = df.withColumn('metadata_file_name', input_file_name())

        col_list = []
        for col in df.schema:
            json_col = col.jsonValue()
            col_list.append('{} {}'.format(json_col['name'], json_col['type'].upper()))

        drop_query = "DROP TABLE IF EXISTS {database}.{name}".format(
            database=database,
            name=self.name)

        self.logger.debug(drop_query)
        hive_context.sql(drop_query)

        tbl_properties_list = [
            "'crawler.file.location'='{s3_location}'",
            "'crawler.file.modified_time'='{modified_time}'",
            "'crawler.file.num_rows'={num_rows}",
            "'crawler.file.num_files'={num_files}",
            "'crawler.file.partitioned'='{partitioned}'",
            "'crawler.file.file_type'='{file_type}'",
            "'crawler.job.process_name'='{process_name}'",
            "'crawler.job.id'='{job_id}'",
            "'crawler.job.directory'='{base_dir}'"
        ]

        if external and file_type == 'csv' and self.use_csv_header:
            tbl_properties_list.append("'skip.header.line.count'='1'")
        
        if self.user_dir:
            tbl_properties_list.append("'crawler.job.user_dir'='{user_dir}'")

        table_properties = "ALTER TABLE {database}.{name} SET TBLPROPERTIES(" + ',\n'.join(tbl_properties_list) + ")"

        if external:
            create_template = """CREATE TABLE IF NOT EXISTS {database}.{name}
                                    ({col_list})
                                    {external_format}
                                    LOCATION '{s3_location}'
                                    """
        else:
            create_template = """CREATE TABLE IF NOT EXISTS {database}.{name}
                                    ({col_list})
                                    """

        create_query = create_template.format(
            database=database,
            name=self.name,
            col_list=','.join(col_list),
            s3_location=self.s3_directory,
            external_format=external_format)

        tbl_properties_update = table_properties.format(
            database=database,
            name=self.name,
            s3_location=self.s3_directory,
            modified_time=self.modified_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            num_rows=df.count(),
            num_files=max(1, len(self.files)),
            partitioned=str(self.partitioned).lower(),
            file_type=file_type,
            job_id=job_id,
            process_name=process_name,
            base_dir=self.crawler_base,
            user_dir=self.user_dir)

        self.logger.debug(create_query)
        hive_context.sql(create_query)

        if not external:
            df.write.mode('overwrite').saveAsTable('{}.{}'.format(database, self.name))

        self.logger.debug(tbl_properties_update)
        hive_context.sql(tbl_properties_update)

        return True

    def repr(self):
        """ object represetation """
        return '<CrawlerTable({}) {}>'.format(self.key, self.name)

class Crawler(object):
    """ Crawler class. Scrapes files from S3 and creates logical tables from them. """
    def __init__(self, bucket, path, s3_client=None, user_dirs=False):
        self.bucket = bucket
        self.path = path
        self.s3_path = 's3n://{}/{}'.format(self.bucket, self.path)
        self.s3 = s3_client
        self.root = Node('{}/{}'.format(self.bucket, self.path))
        self.logger = logging.getLogger(self.__class__.__name__)
        self.user_dirs = user_dirs

        if user_dirs:
            self.user_dir = self.path.rsplit('/', 1)[1]

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
                            node = Node(d, parent=current_root, scraped=False, partitioned=False, file_obj=file, base_file=False)
                            current_root = node

    def get_tables(self, allow_single_files=False):
        """ Using the tree generated by crawl(), build CrawlerTable objects """
        tables = []
        for node in PreOrderIter(self.root):
            if node.is_leaf:
                if node.depth > 2:
                    node.partitioned = True

                if not node.path[1].scraped:
                    if len(node.siblings) == 0 or node.parent == self.root:
                        self.logger.debug('Found single file %s', node.name)
                        parent_path = ''
                        if node.parent != self.root:
                            parent_path = node.parent.name + '/'
                        elif node.parent == self.root and not allow_single_files:
                            self.logger.info('Skipping base file %s', node.name)
                            continue
                        else:
                            node.base_file = True

                        table = CrawlerTable(
                            self.path + '/' + parent_path + node.name,
                            bucket=self.bucket,
                            partitioned=node.partitioned,
                            modified_time=node.file_obj['LastModified'],
                            is_base_file=node.base_file,
                            crawler_base=self.s3_path,
                            user_dir=self.user_dir if self.user_dirs else False
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
                            files=files,
                            crawler_base=self.s3_path,
                            user_dir=self.user_dir if self.user_dirs else False)

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

    crawler_logger = logging.getLogger(Crawler.__name__)
    crawler_logger.setLevel(logging.DEBUG)

    table_logger = logging.getLogger(CrawlerTable.__name__)
    table_logger.setLevel(logging.DEBUG)

    spark_context = SparkContext.getOrCreate()
    hive_context = HiveContext(spark_context)
    spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()

    # Command args
    parser = argparse.ArgumentParser('S3 metastore crawler')
    parser.add_argument('s3_dir', type=str, help='S3 directory to crawl')
    parser.add_argument('--database', type=str, default='default', help='Hive database')
    parser.add_argument('--json_serde', type=str, help='Path to JSONSerde jar')
    parser.add_argument('--process_name', type=str, help='Process name for logging', required=True)
    parser.add_argument('--allow_single_files', default=False, action='store_true',
                        help='Allow single files in the base directory')
    parser.add_argument('--external_tables', default=False, action='store_true',
                        help='Store generated tables as external tables in Hive')
    parser.add_argument('--user_dirs', default=False, action='store_true',
                        help='Store user directory metadata for user schema')
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

    hive_context.sql('CREATE DATABASE IF NOT EXISTS {}'.format(args.database))

    no_urn = args.s3_dir.replace('s3://', '').replace('s3n://', '').replace('s3a://', '')
    bucket = no_urn.split('/', 1)[0]
    key = no_urn.split('/', 1)[1]

    aws_access_key_id = keystore.secret_keys['fs.s3a.access.key'].key.decode()
    aws_secret_access_key = keystore.secret_keys['fs.s3a.secret.key'].key.decode()
    redshift_host = keystore.secret_keys['aws.redshift.host'].key.decode()
    redshift_user = keystore.secret_keys['aws.redshift.user'].key.decode()
    redshift_pass = keystore.secret_keys['aws.redshift.password'].key.decode()
    redshift_database = keystore.secret_keys['aws.redshift.database'].key.decode()

    session = boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='us-west-2')
    s3 = session.client(service_name='s3')
    spark_context._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId', aws_access_key_id)
    spark_context._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey', aws_secret_access_key)

    # Create job id
    redshift_conn = get_redshift_database_connection(redshift_host, redshift_user, redshift_pass, redshift_database)
    update_etlprocess_dtls(redshift_conn, args.process_name, 'start', 0)
    process_details = get_etlprocess_dtls(redshift_conn, args.process_name)
    job_id = process_details['cidw_etl_load_id']

    retries = 0
    retries_allowed = 3
    done = False

    while not done and retries < retries_allowed:
        try:
            dirs = []
            if args.user_dirs:
                for file in s3.list_objects(Bucket=bucket, Prefix=key).get('Contents', []):
                    root_dir = file['Key'].split('/', 1)[0]
                    if key+'/'+root_dir not in dirs:
                        dirs.append(key+'/'+root_dir)
            else:
                dirs.append(key)

            tables = []
            for directory in dirs:
                # Create a file-system tree
                crawler = Crawler(bucket, directory, s3_client=s3, user_dirs=args.user_dirs)
                crawler.crawl()
                crawler.print_tree()

                # Iterate over tree and create logical root-level tables
                tables += crawler.get_tables(allow_single_files=args.allow_single_files,)

            # Import tables into Hive metastore
            for table in tables:
                table.create_hive_table(
                    s3,
                    database=args.database,
                    spark_session=spark_session,
                    hive_context=hive_context,
                    job_id=job_id,
                    process_name=args.process_name,
                    external=args.external_tables
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
            
            done = True

        except Exception as e:
            raise
            retries += 1
            logger.error('Caught exception: %s', str(e))

            if retries >= retries_allowed:
                update_etlprocess_dtls(redshift_conn, args.process_name, 'fail', 0)
                raise
            else:
                time.sleep(random.randint(5, 20))

    # Update etldata tables
    update_etlprocess_dtls(redshift_conn, args.process_name, 'finish', 0)
    update_etlprocess_dtls(redshift_conn, args.process_name, 'process_dtls', 0)

if __name__ == '__main__':
    main()
