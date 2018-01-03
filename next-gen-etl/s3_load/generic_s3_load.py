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

import boto3, py4j, datetime, logging, argparse, jks, py4j, hashlib, random
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext, SparkSession
from pyspark.sql.functions import lit, input_file_name, udf
from pyspark.sql.types import StringType

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

                file_map['s3n://{}/{}'.format(bucket, file['Key'])] = tmp

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
        temp_df.show(1)
        self.df = temp_df
        self.post_count = self.df.count()

    def output_to_data_lake(self, data_lake_location):
        """ Output dataframe to partitioned Parquet data lake """
        dates = self.df.select(self.df['metadata_file_modified'].cast('date').alias('lake_date')).distinct().collect()
        print(dates)
        for date in dates:
            self.logger.debug('Writing %s partition to data lake', str(date['lake_date']))
            out_file = data_lake_location + '/' + self.name + '/' + str(date['lake_date']).replace('-', '/')
            date_df = self.df.filter(self.df['metadata_file_modified'].cast('date') == date['lake_date'])
            date_df.write.mode('overwrite').parquet(out_file)

    def output_to_redshift(self, host, user, password, database, schema, table_prefix='', temp_dir=''):
        """ Output dataframe to a local table on Redshift """
        (self.df.write
         .format("com.databricks.spark.redshift")
         .option("url", "jdbc:redshift://{host}:5439/{database}?user={user}&password={password}&ssl=true".format(
             host=host,
             user=user,
             password=password,
             database=database
         ))
         .option("dbtable", '{schema}.{table_prefix}{name})'.format(
             schema=schema,
             table_prefix=table_prefix,
             name=self.name
         ))
         .option("tempdir", temp_dir)
         .option("forward_spark_s3_credentials", True)
         .mode("error")
         .save())

    def drop_table(self, hive_context):
        """ Drop Hive table """
        drop_table = "DROP TABLE IF EXISTS {database}.{name}".format(
            database=self.database,
            name=self.name
        )

        self.logger.debug(drop_table)
        hive_context.sql(drop_table)

    def __repr__(self):
        """ Object representation """
        return '<HiveTable ({}.{})>'.format(self.database, self.name)

def read_tables_from_hive(hive_context, database):
    """ Get tables from a Hive metastore database """
    tables = []
    hive_context.sql('USE {}'.format(database))
    tables_df = hive_context.sql('show tables')

    for table in tables_df.collect():
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
    parser.add_argument('--job_id', type=str, help='Job ID to put in Hive table metadata')
    parser.add_argument('--data_lake', type=str, help='Data lake location', required=True)
    parser.add_argument('--redshift_schema', type=str, default='sandbox', help='Schema to put Redshift tables in')
    parser.add_argument('--redshift_prefix', type=str, default='', help='Prefix for Redshift tables')
    parser.add_argument('--redshift_temp_dir', type=str, help='Temp dir to use when loading Redshift tables', required=True)
    parser.add_argument('--json_serde', type=str, help='Path to JSONSerde jar')
    args = parser.parse_args()

    keystore = None
    for conf in spark_context._conf.getAll():
        if conf[0] == 'spark.hadoop.hadoop.security.credential.provider.path':
            keystore = jks.KeyStore.load(conf[1].replace('jceks://file', ''), 'none')

    if not keystore:
        raise AttributeError('spark.hadoop.hadoop.security.credential.provider.path is required')

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

    #spark_context._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId', aws_access_key_id)
    #spark_context._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey', aws_secret_access_key)
    args.data_lake = args.data_lake.replace('s3://', 's3n://').replace('s3a://', 's3n://')
    args.redshift_temp_dir = args.redshift_temp_dir.replace('s3://', 's3n://').replace('s3a://', 's3n://')

    # Generate a job ID if one isn't provided
    if not args.job_id:
        args.job_id = 'jr_{}'.format(
            hashlib.sha256((datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.')+
                            str(random.randint(1, 1000))).encode()).hexdigest()
        )
    
    logger.info('Starting job %s', args.job_id)

    # temporary hard-coded last-extract date
    # TODO: read last_extract_date from job meta table
    last_extract_date = datetime.datetime(2017, 11, 30)

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

            table.output_to_data_lake(args.data_lake)
            try:
                table.output_to_redshift(
                    redshift_host,
                    redshift_user,
                    redshift_pass,
                    redshift_database,
                    args.redshift_schema,
                    table_prefix=args.redshift_prefix,
                    temp_dir=args.redshift_temp_dir)
            except py4j.protocol.Py4JJavaError as e:
                logger.error('Caught Redshift error %s', str(e))

            if table.pre_count != table.post_count:
                logger.error('Sink count does not match source count: %s vs %s',
                             str(table.pre_count),
                             str(table.post_count))

    logger.info('Finished job %s', args.job_id)

if __name__ == '__main__':
    main()
