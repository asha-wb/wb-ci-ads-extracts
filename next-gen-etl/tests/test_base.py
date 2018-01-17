#!/usr/bin/python

import os
import csv
import json
import gzip
from io import StringIO
import pandas
import boto3
import jks

class TestBaseClass(object):
    dummy_content = [
        {"id": 1, "name": "Testy McTestFace", "description": "test"},
        {"id": 2, "name": "Foghorn Leghorn", "description": "test"}
    ]
    s3_bucket = 'dev-cmdt-user'
    test_dir = 'btelle/unit_tests'
    tast_tables = 4

    def get_credentials(self):
        try:
            credential_file = os.environ['CREDENTIAL_FILE']
        except KeyError:
            raise Exception('Credential file must be supplied in env variable CREDENTIAL_FILE')

        keystore = jks.KeyStore.load(credential_file.replace('jceks://file', ''), 'none')

        ret = {}
        for key in keystore.secret_keys:
            ret[key] = keystore.secret_keys[key].key.decode()

        return ret

    def try_build_test_s3_directory(self, spark_context=None, user_dir=''):
        try:
            credentials = self.get_credentials()
            aws_access_key_id = credentials['fs.s3a.access.key']
            aws_secret_access_key = credentials['fs.s3a.secret.key']
        except KeyError:
            raise Exception('AWS credentials are required in jceks format in CREDENTIAL_FILE')

        session = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name='us-west-2')
        self.s3 = session.client(service_name='s3')

        if spark_context:
            spark_context._jsc.hadoopConfiguration().set('fs.s3a.access.key', aws_access_key_id)
            spark_context._jsc.hadoopConfiguration().set('fs.s3a.secret.key', aws_secret_access_key)
            spark_context._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId', aws_access_key_id)
            spark_context._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey', aws_secret_access_key)
            spark_context._jsc.hadoopConfiguration().set('fs.s3a.server-side-encryption-algorithm', 'AES256')

        if user_dir:
            user_dir += '/'

        try:
            obj = self.s3.get_object(Bucket=self.s3_bucket, Key=self.test_dir+'/incoming/'+user_dir+'test_csv/test_csv.csv')
            return True
        except self.s3.exceptions.NoSuchKey:
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=self.test_dir+'/incoming/'+user_dir+'test_csv/test_csv.csv',
                Body=self.get_csv_string(),
                ServerSideEncryption='AES256'
            )

            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=self.test_dir+'/incoming/'+user_dir+'test_json/test_json.json',
                Body=self.get_json_string(),
                ServerSideEncryption='AES256'
            )

            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=self.test_dir+'/incoming/'+user_dir+'test_gzip/test_gzip.gz',
                Body=self.get_gzip_string(),
                ServerSideEncryption='AES256'
            )

            parquet_body = self.get_parquet_string()
            if parquet_body:
                self.s3.put_object(
                    Bucket=self.s3_bucket,
                    Key=self.test_dir+'/incoming/'+user_dir+'test_parquet/test_parquet.parquet',
                    Body=parquet_body,
                    ServerSideEncryption='AES256'
                )
            else:
                print('Skipping parquet file, no fastparquet library available')

            return True
    
    def get_csv_string(self):
        fh = StringIO()
        writer = csv.DictWriter(fh, self.dummy_content[0].keys())
        writer.writeheader()

        for row in self.dummy_content:
            writer.writerow(row)

        contents = fh.getvalue()
        fh.close()
        return contents
    
    def get_json_string(self):
        contents = ''

        for row in self.dummy_content:
            contents += json.dumps(row) + "\n"
        
        return contents

    def get_parquet_string(self):
        df = pandas.DataFrame.from_records(self.dummy_content, columns=self.dummy_content[0].keys())

        try:
            import fastparquet
            filename = './__test_parquet_output.parquet'
            fastparquet.write(filename, df)
            with open(filename, 'rb') as fh:
                contents = fh.read()
            os.remove(filename)
        except ImportError:
            contents = None

        return contents

    def get_gzip_string(self):
        csv = self.get_csv_string().encode()

        filename = './__test_gzip_output.gz'
        with gzip.GzipFile(filename=filename, mode='w') as gzip_file:
            gzip_file.write(csv)

        with open(filename, 'rb') as fh:
            contents = fh.read()

        os.remove(filename)
        return contents
