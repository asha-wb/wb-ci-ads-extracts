import sys, boto3, py4j, datetime, logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit, input_file_name, udf
from pyspark.sql.types import StringType
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
#from watchtower import CloudWatchLogHandler

args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])

# debugging
#import pydevd
#pydevd.settrace('169.254.76.0', port=9001, stdoutToServer=True, stderrToServer=True)

session = boto3.session.Session(region_name='us-west-2')
#cw_stream = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%m:%sZ_')+args['JOB_NAME'].replace(' ', '_')
#cw_handler = CloudWatchLogHandler(log_group='dev_glue_logs', stream_name=cw_stream, boto3_session=session)
console_handler = logging.StreamHandler(sys.stdout)

# logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)
#logger.addHandler(cw_handler)

# contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sql_c = SQLContext(sc)

glue_client = boto3.client(service_name='glue', region_name='us-west-2')
s3 = boto3.client(service_name='s3', region_name='us-west-2')

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# temporary hard-coded last-extract date
# TODO: read last_extract_date from job meta table
last_extract_date = datetime.datetime(2017, 11, 30)

# read data catalog
glue_retry = True
retries = 0
while glue_retry:
    try:
        tables = glue_client.get_tables(DatabaseName='fandango_demo')
        glue_retry = False
    except Exception:
        retries += 1

        if retries > 2:
            glue_retry = False
            logger.error('Get tables from Glue failed.')

if 'TableList' in tables:
    for table in tables['TableList']:
        datasource0 = glueContext.create_dynamic_frame.from_catalog(database="fandango_demo", table_name=table['Name'],
                                                                    transformation_ctx="datasource0")

        source_row_count = datasource0.count()
        logger.info('Source row count: {}'.format(source_row_count))

        # column transformations
        mappings = []
        for col in table['StorageDescriptor']['Columns']:
            if col['Type'].lower() == 'string':
                # Find a sane column length... max(col) + 25% maybe
                mappings.append((col['Name'], col['Type'], col['Name'], 'varchar'))

        if mappings:
            applymapping1 = ApplyMapping.apply(frame=datasource0, mappings=mappings, transformation_ctx="applymapping1")
        else:
            applymapping1 = datasource0

        # resolve column types
        resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                             transformation_ctx="resolvechoice2")

        # drop null fields
        dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")

        # build s3 file map
        s3_url = table['StorageDescriptor']['Location']
        bucket = s3_url.replace('s3://', '').split('/')[0]
        prefix = s3_url.replace('s3://', '').split('/', 1)[1]

        file_map = {}
        for file in s3.list_objects(Bucket=bucket, Prefix=prefix)['Contents']:
            if file['Key'] != prefix:
                tmp = {}
                tmp['owner'] = file['Owner']['DisplayName']
                tmp['filename'] = file['Key']
                tmp['modified_time'] = file['LastModified'].replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')

                file_map['s3://{}/{}'.format(bucket, file['Key'])] = tmp

        # udf to get metadata from file map
        def get_attr_from_file_map(key, attr):
            if key in file_map and attr in file_map[key]:
                return file_map[key][attr]
            return None

        get_attr = udf(get_attr_from_file_map, StringType())

        temp_df = dropnullfields3.toDF()
        temp_df = temp_df.withColumn('metadata_file_name', input_file_name())

        # hack to persist metadata_file_name
        tmp_s3_out_path = args['TempDir']+'/hack-format-output/'+datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')+'/'+table['Name']
        temp_df.write.parquet(tmp_s3_out_path)
        temp_df = sql_c.read.parquet(tmp_s3_out_path)

        # metadata columns
        temp_df = (
            temp_df.withColumn('metadata_file_owner', get_attr(temp_df['metadata_file_name'], lit('owner')))
                   .withColumn('metadata_file_modified', get_attr(temp_df['metadata_file_name'], lit('modified_time')).cast('timestamp'))
                   .withColumn('metadata_data_classification', lit(table['StorageDescriptor']['Parameters']['classification']))
                   .withColumn('metadata_data_compression',lit(table['StorageDescriptor']['Parameters']['compressionType']))
                   .withColumn('metadata_record_count', lit(table['StorageDescriptor']['Parameters']['recordCount']))
            )

        # only keep rows modified since last_extract_date
        temp_df = temp_df.filter(temp_df['metadata_file_modified'] >= lit(last_extract_date))

        additionalcolumns4 = DynamicFrame.fromDF(temp_df, glueContext, "additionalcolumns4")

        keep_trying = True
        retries = 0
        while keep_trying:
            try:
                # TODO: De-dupe

                # write to data lake
                dates = temp_df.select(temp_df['metadata_file_modified'].cast('date').alias('lake_date')).distinct().collect()

                for date in dates:
                    out_file = args['TempDir'] + '/data-lake/' + table['Name'] + '/' + str(date['lake_date']).replace('-', '/')
                    date_df = temp_df.filter(temp_df['metadata_file_modified'].cast('date') == date['lake_date'])
                    date_df.write.mode('overwrite').parquet(out_file)

                # write to redshift
                datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=additionalcolumns4,
                                                                           catalog_connection="Test Redshift Direct",
                                                                           connection_options={
                                                                               "dbtable": 'sandbox.autogen_' + table['Name'],
                                                                               "database": "devcidw"},
                                                                           redshift_tmp_dir=args["TempDir"]+'redshift-tmp/'+datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')+'/'+table['Name'],
                                                                           transformation_ctx="datasink5")

                keep_trying = False
                logger.info('Ingested {}'.format(table['Name']))

                # auditing row counts
                # reconcile_row_count_dyf = glueContext.create_dynamic_frame_from_options(
                #     connection_type='redshift',
                #     catalog_connection='Test Redshift Direct',
                #     connection_options={
                #         'dbtable': 'sandbox.autogen_' + table['Name'],
                #         'database': 'devcidw',
                #         'tmp_dir': args["TempDir"],
                #         'redshift_tmp_dir': args["TempDir"]
                #     },
                #     redshift_tmp_dir=args["TempDir"],
                #     tmp_dir=args["TempDir"]
                # )

                sink_row_count = additionalcolumns4.count()
                logger.info('Sink row count: {}'.format(sink_row_count))

                if source_row_count != sink_row_count:
                    logger.error('Row count mismatch: source: {}, sink: {}'.format(source_row_count, sink_row_count))

                # TODO: archive file
            except py4j.protocol.Py4JJavaError as e:
                logger.error(e)
                retries += 1
                if retries > 2:
                    keep_trying = False
                    logger.error('Max connection failure retries hit, continuing...')

job.commit()
