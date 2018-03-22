#! /usr/bin/python

"""
Generic S3 -> ODS load. Reads tables from Hive metastore.

Usage: spark-submit \
        --conf spark.hadoop.hadoop.security.credential.provider.path jceks://file/path/to/jceks \
        generic_s3_load.py \
            --data_lake s3://my-bucket/path/to/data-lake \
            --redshift_temp_dir s3://my-bucket/path/to/tmp \
            [ --redshift_schema sandbox ] \
            [ --redshift_prefix prefix_ ] \
            [ --database default ] \
            [ --json_serde hdfs://path/to/jars/hive-hcatalog-core.jar ] \
            [ --job_id job_id ]
"""

import boto3, py4j, datetime, logging, argparse, jks, py4j, hashlib, random, time, os, json
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext, SparkSession, Column
from pyspark.sql.functions import lit, input_file_name, udf, col
from pyspark.sql.types import StringType
from utils import get_redshift_database_connection, update_etlprocess_dtls, get_etlprocess_dtls, update_file_dtls

# column metadata
# https://medium.com/@andyreagan/should-i-set-metadata-manually-in-pyspark-39993cd55359
def withMeta(self, alias, meta):
    sc = SparkContext._active_spark_context
    jmeta = sc._gateway.jvm.org.apache.spark.sql.types.Metadata
    return Column(getattr(self._jc, "as")(alias, jmeta.fromJson(json.dumps(meta))))

Column.withMeta = withMeta

class HiveTable(object):
    """ Table from Hive metastore """
    def __init__(self, name, database):
        self.name = name
        self.database = database
        self.properties = {}
        self.df = None

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug('Initializing %s', self.name)

    def add_property(self, key, value):
        """ Add a property to the table's properties """
        self.logger.debug('Setting %s = %s', key, value)
        self.properties[key] = value

    def get_property(self, key):
        """ Get a preoperty's value """
        return self.properties.get(key, None)

    def build_dataframe(self, hive_context):
        """ Build a dataframe from Hive table """
        select_query = """
            SELECT * 
            FROM {database}.{table}""".format(
                database=self.database,
                table=self.name
            )

        self.df = hive_context.sql(select_query)
        self.pre_count = self.df.count()

        self.logger.debug('Built dataframe with %s rows', self.pre_count)

    def add_meta_columns(self, s3_client):
        """ Add metadata columns to dataframe """
        # build s3 file map
        s3_url = self.get_property('crawler.file.location')
        bucket = s3_url.replace('s3n://', '').split('/')[0]
        prefix = s3_url.replace('s3n://', '').split('/', 1)[1]

        file_map = {}
        for file in s3_client.list_objects(Bucket=bucket, Prefix=prefix)['Contents']:
            if file['Key'] != prefix:
                tmp = {}
                tmp['owner'] = file['Owner']['DisplayName']
                tmp['filename'] = file['Key']
                tmp['modified_time'] = file['LastModified'].replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')
                tmp['size'] = file['Size']

                file_map['s3n://{}/{}'.format(bucket, file['Key'])] = tmp

        self.file_map = file_map

        # udf to get metadata from file map
        def get_attr_from_file_map(key, attr):
            if key in file_map and attr in file_map[key]:
                return file_map[key][attr]
            return None

        get_attr = udf(get_attr_from_file_map, StringType())

        # metadata columns
        temp_df = (
            self.df.withColumn('metadata_file_owner', get_attr(self.df['metadata_file_name'], lit('owner')))
                    .withColumn('metadata_file_modified', get_attr(self.df['metadata_file_name'], lit('modified_time')).cast('timestamp'))
                    .withColumn('metadata_data_classification', lit(self.get_property('crawler.file.file_type')))
                    .withColumn('metadata_record_count', lit(self.get_property('crawler.file.num_rows')))
            )

        self.df = temp_df
        self.post_count = self.df.count()

    def output_to_data_lake(self, data_lake_location):
        """ Output dataframe to partitioned Parquet data lake """
        dates = self.df.select(self.df['metadata_file_modified'].cast('date').alias('lake_date')).distinct().collect()

        for date in dates:
            self.logger.debug('Writing %s partition to data lake', str(date['lake_date']))
            out_file = data_lake_location + '/' + self.name + '/' + str(date['lake_date']).replace('-', '/')
            date_df = self.df.filter(self.df['metadata_file_modified'].cast('date') == date['lake_date'])
            date_df.write.mode('overwrite').parquet(out_file)

    def output_to_redshift(self, host, user, password, database, schema, table_prefix='', temp_dir='', spark_context=None):
        """ Output dataframe to a local table on Redshift """
        self.logger.info('Creating %s.%s%s', schema, table_prefix, self.name)

        # Calculate column lengths
        if spark_context:
            sql_c = SQLContext(spark_context)
            sql_c.registerDataFrameAsTable(self.df, self.name)
            query = "SELECT max(length({col})) as maxlength FROM {name}"

            adjust_cols = []
            for column in self.df.schema.fields:

                if type(column.dataType) == StringType:
                    adjust_cols.append(column)

            for column in adjust_cols:
                exec_query = query.format(
                    col=column.name,
                    name=self.name
                
                )
                self.logger.debug(exec_query)

                maxlength = int(sql_c.sql(exec_query).collect()[0][0])
                
                # TODO: this *should be* unnecessary, but the load fails unless columns are significantly 
                # wider than needed. Redshift bug?
                maxlength += int(maxlength * 0.3)

                self.logger.info('Setting %s maxlength=%s', column.name, maxlength)
                self.df = self.df.withColumn(column.name, self.df[column.name].withMeta("", {"maxlength": maxlength}))

        (self.df.write
         .format("com.databricks.spark.redshift")
         .option("url", "jdbc:redshift://{host}:5439/{database}?user={user}&password={password}&ssl=true".format(
             host=host,
             user=user,
             password=password,
             database=database
         ))
         .option("dbtable", '{schema}.{table_prefix}{name}'.format(
             schema=schema,
             table_prefix=table_prefix,
             name=self.name
         ))
         .option("tempdir", temp_dir)
         .option("forward_spark_s3_credentials", True)
         .mode("append")
         .save())

    def drop_table(self, hive_context):
        """ Drop Hive table """
        drop_table = "DROP TABLE IF EXISTS {database}.{name}".format(
            database=self.database,
            name=self.name
        )

        self.logger.debug(drop_table)
        hive_context.sql(drop_table)
    
    def archive_files(self, archive_dir, s3_client=None, redshift_conn=None, process_details={}):
        """ Move table files to archive bucket """
        files = self.file_map.keys()
        date_dir = datetime.datetime.utcnow().strftime('%Y/%m/%d/%H')
        base_dir = self.get_property('crawler.job.directory').replace('s3://', 's3n://')

        no_urn = archive_dir.replace('s3://', '').replace('s3n://', '').replace('s3a://', '')
        dest_bucket = no_urn.split('/', 1)[0]
        dest_key = no_urn.split('/', 1)[1]

        for file in files:
            start_time = datetime.datetime.now()
            no_urn = file.replace('s3://', '').replace('s3n://', '').replace('s3a://', '')
            src_bucket = no_urn.split('/', 1)[0]
            src_key = no_urn.split('/', 1)[1]

            path = ''
            if base_dir != '':
                path = file.replace(base_dir, '')
                if path[0] == '/':
                    path = path[1:]

            s3_client.copy_object(
                Bucket=dest_bucket,
                Key=dest_key + '/' + date_dir + '/' + path,
                CopySource={
                    'Bucket': src_bucket,
                    'Key': src_key
                },
                ServerSideEncryption='AES256'
            )

            s3_client.delete_object(
                Bucket=src_bucket,
                Key=src_key
            )

            end_time = datetime.datetime.now()

            if redshift_conn:
                update_file_dtls(
                    conn=redshift_conn,
                    cidw_etlload_id=process_details['cidw_etl_load_id'],
                    cidw_etlprocess_id=process_details['cidw_process_id'],
                    processname=process_details['process_name'],
                    context='extract',
                    source=src_bucket + '/' + src_key,
                    destination=dest_bucket + '/' + dest_key,
                    filename=os.path.basename(src_key),
                    size_bytes=self.file_map[file].get('size', 0),
                    rows_processed=self.get_property('crawler.file.num_rows'),
                    transfer_status='S',
                    transfer_dtm=end_time.strftime('%Y-%m-%d %H:%M:%S'),
                    duration_seconds=(end_time-start_time).seconds,
                    cidw_batch_id=process_details['cidw_batch_id']
                )

            self.logger.info('Moved s3://%s/%s to s3://%s/%s/%s/%s',
                             src_bucket, src_key,
                             dest_bucket, dest_key,
                             date_dir, path)

    def __repr__(self):
        """ Object representation """
        return '<HiveTable ({}.{})>'.format(self.database, self.name)

def read_tables_from_hive(hive_context, database):
    """ Get tables from a Hive metastore database """
    tables = []
    hive_context.sql('USE {}'.format(database))
    tables_df = hive_context.sql('show tables')

    for table in tables_df.collect():
        print(table)
        tmp_table = HiveTable(table.tableName, table.database)

        tblproperties = "show tblproperties {database}.{name}".format(
            database=table.database,
            name=table.tableName)

        details = hive_context.sql(tblproperties)
        for row in details.collect():
            tmp_table.add_property(row.key, row.value)

        tables.append(tmp_table)
    return tables

def main():
    """ Main application logic """
    # logging
    logging.basicConfig(format='[%(levelname)s] %(asctime)s %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    table_logger = logging.getLogger(HiveTable.__name__)
    table_logger.setLevel(logging.DEBUG)

    # contexts
    spark_context = SparkContext.getOrCreate()
    hive_context = HiveContext(spark_context)

    # args
    parser = argparse.ArgumentParser('Generic S3 to ODS load')
    parser.add_argument('--database', type=str, default='default', help='Hive database to read tables from')
    parser.add_argument('--process_name', type=str, help='Process name for logging', required=True)
    parser.add_argument('--data_lake', type=str, help='Data lake location', required=True)
    parser.add_argument('--redshift_schema', type=str, default='sandbox', help='Schema to put Redshift tables in')
    parser.add_argument('--redshift_prefix', type=str, default='', help='Prefix for Redshift tables')
    parser.add_argument('--redshift_temp_dir', type=str, help='Temp dir to use when loading Redshift tables', required=True)
    parser.add_argument('--json_serde', type=str, help='Path to JSONSerde jar')
    parser.add_argument('--clean_up', default=False, action='store_true',
                        help='Move data files to archive dir after crawling')
    parser.add_argument('--archive_dir', type=str, help='Directory to archive files to')
    args = parser.parse_args()

    keystore = None
    for conf in spark_context._conf.getAll():
        if conf[0] == 'spark.hadoop.hadoop.security.credential.provider.path':
            keystore = jks.KeyStore.load(conf[1].replace('jceks://file', ''), 'none')

    if not keystore:
        raise AttributeError('spark.hadoop.hadoop.security.credential.provider.path is required')
    
    if args.clean_up:
        if not args.archive_dir:
            raise AttributeError('archive_dir is required for clean up')

        if args.archive_dir[0:2] != 's3' or '://' not in args.archive_dir:
            raise AttributeError('archive_dir must be in the format s3://<bucket>/<key>/')

    # Hack to get json serde working. Shouldn't be necessary on hortonworks cluster
    if args.json_serde:
        hive_context.sql('ADD JAR {}'.format(args.json_serde))

    try:
        aws_access_key_id = keystore.secret_keys['fs.s3a.access.key'].key.decode()
        aws_secret_access_key = keystore.secret_keys['fs.s3a.secret.key'].key.decode()
        redshift_host = keystore.secret_keys['aws.redshift.host'].key.decode()
        redshift_user = keystore.secret_keys['aws.redshift.user'].key.decode()
        redshift_pass = keystore.secret_keys['aws.redshift.password'].key.decode()
        redshift_database = keystore.secret_keys['aws.redshift.database'].key.decode()
    except IndexError as e:
        logger.error('Missing required credential %s', str(e))
        raise

    session = boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='us-west-2')
    s3 = session.client(service_name='s3')
 
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.access.key', aws_access_key_id)
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.secret.key', aws_secret_access_key)
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.server-side-encryption-algorithm', 'AES256')
    args.data_lake = args.data_lake.replace('s3://', 's3a://').replace('s3n://', 's3a://')
    args.redshift_temp_dir = args.redshift_temp_dir.replace('s3://', 's3a://').replace('s3n://', 's3a://')

    # Create job id
    redshift_conn = get_redshift_database_connection(redshift_host, redshift_user, redshift_pass, redshift_database)
    update_etlprocess_dtls(redshift_conn, args.process_name, 'start', 0)
    process_details = get_etlprocess_dtls(redshift_conn, args.process_name)
    process_details['process_name'] = args.process_name
    job_id = process_details['cidw_etl_load_id']

    last_extract_date = process_details['cidw_lastcompletiondttm']
    if last_extract_date is None:
        last_extract_date = datetime.datetime.now() - datetime.timedelta(days=1)
    
    logger.info('Last extract date: %s', last_extract_date)

    retries = 0
    retries_allowed = 3
    done = False

    while not done and retries < retries_allowed:
        try:
            # read data catalog
            tables = read_tables_from_hive(hive_context, args.database)

            for table in tables:
                print(table.properties)
                table_updated_at = datetime.datetime.strptime(
                    table.get_property('crawler.file.modified_time'),
                    '%Y-%m-%dT%H:%M:%SZ')

                if table_updated_at >= last_extract_date:
                    table.build_dataframe(hive_context)
                    table.add_meta_columns(s3)

                    data_lake_dir = args.data_lake
                    if table.get_property('crawler.job.user_dir'):
                        data_lake_dir += '/' + table.get_property('crawler.job.user_dir')

                    table.output_to_data_lake(data_lake_dir)
                    try:
                        redshift_schema = args.redshift_schema
                        if table.get_property('crawler.job.user_dir'):
                            redshift_schema = table.get_property('crawler.job.user_dir')

                        table.output_to_redshift(
                            redshift_host,
                            redshift_user,
                            redshift_pass,
                            redshift_database,
                            redshift_schema,
                            table_prefix=args.redshift_prefix,
                            temp_dir=args.redshift_temp_dir,
                            spark_context=spark_context)
                    except py4j.protocol.Py4JJavaError as e:
                        logger.error('Caught Redshift error %s', str(e))

                    if table.pre_count != table.post_count:
                        logger.error('Sink count does not match source count: %s vs %s',
                                     str(table.pre_count),
                                     str(table.post_count))
                    elif args.clean_up:
                        table.drop_table(hive_context)
                        table.archive_files(
                            args.archive_dir,
                            s3_client=s3,
                            redshift_conn=redshift_conn,
                            process_details=process_details
                        )
            done = True
        except Exception as e:
            retries += 1
            logger.error('Caught exception: %s', str(e))

            if retries >= retries_allowed:
                update_etlprocess_dtls(redshift_conn, args.process_name, 'fail', 0)
                raise
            else:
                time.sleep(random.randint(5, 20))

    update_etlprocess_dtls(redshift_conn, args.process_name, 'finish', 0)
    update_etlprocess_dtls(redshift_conn, args.process_name, 'process_dtls', 0)

if __name__ == '__main__':
    main()
