import unittest, pydevd, sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext

class TestConnections(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestConnections, self).__init__(*args, **kwargs)

        self.sc = SparkContext.getOrCreate()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session

    def test_s3_connection(self):
        s3_dyf = self.glueContext.create_dynamic_frame.from_catalog(database="fandango_demo", table_name='fd_campaign')
        self.assertGreater(s3_dyf.count(), 0)

    def test_redshift_connection(self):
        rs_dyf = self.glueContext.create_dynamic_frame.from_catalog(database="redshift", table_name="redshift_crawlerdevcidw_sandbox_btelle_fd_tblcustomer_10k", redshift_tmp_dir='s3://dev-bi-users/aws-glue-temp')
        self.assertGreater(rs_dyf.count(), 0)

if __name__ == '__main__':
    pydevd.settrace('169.254.76.0', port=9001, stdoutToServer=True, stderrToServer=True)

    # remove glue's args
    for i in range(1, len(sys.argv)):
        sys.argv.pop()

    unittest.main()
