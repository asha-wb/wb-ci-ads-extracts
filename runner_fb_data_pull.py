#!/usr/bin/env python2
#sand_fb_extract.py
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


import csv
import requests
from datetime import datetime
from collections import defaultdict
from facebookads.api import FacebookAdsApi
from facebookads.adobjects.adaccount import AdAccount
#from facebookads.adobjects.adsinsights import AdsInsights

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
AD_ACCOUNT_ID = 'act_186520528512505'  # act_<account_id>
#APP_ID = '170628176822745'
APP_SECRET = ''
EXTENDED_USER_TOKEN = ''
        # contains app token embedded inside it


def main():
    """
    main method
    """
    #appsecret_proof = gen_app_secret_proof(app_secret, access_token)
    # FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)


    # Step-0: test token expiry here
    FacebookAdsApi.init(app_secret=APP_SECRET,
                        access_token=EXTENDED_USER_TOKEN)
                        # looks like app_id is not reqd.
                        # since we are providing extended_user_token
                        # which already has app token embedded in it

    # Step-1: Pull data from API
    fields = [
          'account_id'
        , 'account_name'
        , 'campaign_id'
        , 'campaign_name'
        , 'adset_id'
        , 'adset_name'
        , 'ad_id'
        , 'ad_name'
        , 'date_start'
        , 'date_stop'
        , 'impressions'
        , 'frequency'
        , 'reach'
        , 'spend'
        , 'clicks'
        , 'outbound_clicks'
        , 'unique_outbound_clicks'
        , 'inline_link_clicks'
        , 'unique_inline_link_clicks'
        , 'social_impressions'
        , 'social_reach'
        , 'video_avg_time_watched_actions'
        , 'video_avg_percent_watched_actions'
        , 'video_10_sec_watched_actions'
        , 'video_p100_watched_actions'
        , 'actions'
        ]
    params = {
        'level': 'ad',
        'filtering': [],
        'breakdowns': ['age', 'gender'],
        'time_increment':1,
        'time_range': {'since':'2017-09-15', 'until':'2017-10-15'},
        'action_breakdowns': ['action_type'],  # optional, as it is the default behavior
        'export_format': 'csv',  # not working
        'export_name': 'fb_export.csv'  # not working
        }

    account = AdAccount(fbid=AD_ACCOUNT_ID)
    print("Account ID for which report is being pulled = %s" %(account.get_id()))
    fb_cursor = account.get_insights(fields=fields, params=params)  # returns facebook.ads.Cursor object
    print("Request successfully sent to Graph API...")

    # Step-2: flatten the graph api output
    buff = []
    for ai_obj in fb_cursor:
        data = ai_obj.export_all_data()
        flat_data = defaultdict()
        for k, v in data.iteritems():
            if not isinstance(v, list):
                flat_data[k] = v
                continue
            elif k == 'actions':
                for each_action in v:
                    a_key = each_action.get('action_type')  # currently the code is designed to only capture action_type
                    a_val = each_action.get('value')
                    flat_data[a_key] = a_val
            elif k.startswith('video'):
                a_key = k
                if v[0]['action_type'] == 'video_view':  # currently code is only designed to capture the first dict
                    a_val = v[0]['value']
                    flat_data[a_key] = a_val
            else:
                flat_data[k] = str(v)

        buff.append(flat_data)
    print("data is flattened...")

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


