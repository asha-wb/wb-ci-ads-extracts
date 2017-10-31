#!/usr/bin/env python2
#runner_fb_data_pull.py
# -*- coding: utf-8 -*-
"""
POC for Facebook analytics data pull:-
https://wbdatasquad.atlassian.net/browse/CIAP-1

Step 1: Pull "Insights" data from FB Marketing API
Step 2: Flatten the output
Step 3: Write the output to a csv file
Step 4(TBD): Load this data to a staging S3 bucket
Step 5(TBD): Ingest data into Redshift
"""

import os
import sys
import csv
import logging
import logging.config  # required
import argparse
import configparser
from datetime import datetime
from collections import defaultdict

import pandas as pd
import requests
import boto3
from facebookads import FacebookSession
from facebookads import FacebookAdsApi
from common.log import setup_logging
from common.redshift_connector import DBConnection
from common.kms_utils import decrypt_content
from common.s3_uploader import upload_file_s3
from lib.read_objects_async import get_insights
from lib.token_utils import get_extended_access_token

logger = logging.getLogger('fb.markering_api.runner')

#import hashlib
#import hmac
#
#def gen_app_secret_proof(app_secret, access_token):
#    h = hmac.new(
#        app_secret.encode('utf-8'),
#        msg=access_token.encode('utf-8'),
#        digestmod=hashlib.sha256
#    )
#    return h.hexdigest()

OUT_FILE = 'output/fb_extract_%s.csv' % (datetime.now().strftime('%Y%m%d_%H%M'))


def main():
    """
    main method
    """
    # general setup: logging and read config/inputs
    parser = argparse.ArgumentParser(description='Facebook Marketing API extracts daily run')
    config = configparser.ConfigParser()

    parser.add_argument("--config_file")
    parser.add_argument("--encrypted_data_key_file")
    parser.add_argument("--encrypted_credentials_file")
    args = parser.parse_args()

    config.read(args.config_file)

    # Step-0: initiate aws and facebook app sessions
    aws_session = boto3.Session(profile_name=config.get('AWS', 'profile'))
    aws_access_key = aws_session.get_credentials().access_key
    aws_secret_key = aws_session.get_credentials().secret_key
    redshift = config['Redshift']

    app_id = config.get('Facebook', 'app_id')
    app_secret = config.get('Facebook', 'app_secret')
    access_token = config.get('Facebook', 'access_token')

    new_access_token = get_extended_access_token(app_id, app_secret, access_token)

    fb_session = FacebookSession(
            app_id,
            app_secret,
            new_access_token
    )
    api = FacebookAdsApi(fb_session)

    FacebookAdsApi.set_default_api(api)

    start_date = datetime.strptime(config.get('Facebook', 'start_date'), '%Y-%m-%d')
    end_date = datetime.strptime(config.get('Facebook', 'end_date'), '%Y-%m-%d')

    # Step-1: Call the Insights API async
    output_df = get_insights(
            start_date,
            end_date,
            account_id,
            new_access_token
            )


    # Step-3: write the output to a file
    with open(OUT_FILE, 'wb') as fout:
        fieldnames = []
        for x in buff:
            fieldnames.extend(x.keys())
        #fieldnames.extend([x.keys() for x in buff])
        fieldnames = list(set(fieldnames))
        writer = csv.DictWriter(fout, fieldnames=fieldnames, dialect='excel')
        writer.writeheader()
        writer.writerows(buff)

    print("output file is written - %s" % (OUT_FILE))


if __name__ == "__main__":
    print("Start FB data pull...")
    main()
    print("End FB data pull...exiting")


