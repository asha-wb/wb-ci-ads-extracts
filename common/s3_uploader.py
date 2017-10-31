import boto3
import logging


logger = logging.getLogger(__name__)


def upload_file_s3(session, filename, dest_bucket):
    s3 = session.client('s3')
    bucket = dest_bucket.partition('/')[0]
    key = dest_bucket.partition('/')[-1]
    file = filename.split('/')[-1]
    try:
        s3.upload_file(filename, bucket, key+file, ExtraArgs={'ServerSideEncryption': "AES256"})
    except:
        logger.exception('upload to S3 failed for file %s' % (filename))
        raise
    else:
        logger.info("file %s uploaded to S3" % (file))
        return
