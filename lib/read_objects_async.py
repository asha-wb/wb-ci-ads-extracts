# -*- coding: utf-8 -*-
#lib/read_objects_async.py

import json
import time
import logging
import requests
import pandas as pd
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.adreportrun import AdReportRun
from lib.flatten_fb_insights import json_to_flatten

logger = logging.getLogger(__name__)

STATUS_URL = "https://graph.facebook.com/v2.10/"


def transform_action_array(arr):
    """ Transform an action or action_value array into a record structure. """

    ret = {}
    for row in arr:
        ret[row['action_type']] = row['value']

    return ret


def get_insights(
        start_date,
        end_date,
        account_id,
        access_token,
        columns_order):
    """
    """
    print('Starting get insights')

    output_df = pd.DataFrame(columns=['date_start', 'date_stop', 'ad_id', 'ad_name', 'adset_id','adset_name', 'account_id', 'account_name', 'campaign_id','campaign_name', 'age', 'gender', 'video_avg_time_watched_actions_0_value','impressions', 'spend', 'page_engagement', 'like', 'post_engagement','post', 'comment', 'post_reaction', 'video_avg_percent_watched_actions_0_value','outbound_clicks_0_value', 'unique_outbound_clicks_0_value', 'clicks', 'link_click','unique_inline_link_clicks', 'video_p25_watched_actions_0_value','video_p50_watched_actions_0_value', 'video_p75_watched_actions_0_value','video_p95_watched_actions_0_value', 'video_p100_watched_actions_0_value','video_p10_watched_actions_0_value', 'video_30_sec_watched_actions_0_value', 'frequency', 'reach', 'social_impressions', 'social_reach','call_to_action_clicks'])
    ad_account_id = 'act_' + str(account_id)

    acc = AdAccount(ad_account_id)

    params = {}
    params['access_token'] = access_token
    params['level'] = 'ad'
    params['breakdowns'] = ['age', 'gender']
    params['time_increment'] = 1
    params['fields'] = ['account_id',
                        'account_name',
                        'ad_id',
                        'ad_name',
                        'adset_id',
                        'adset_name',
                        'campaign_id',
                        'campaign_name',
                        'video_avg_time_watched_actions',
                        'impressions',
                        'spend',
                        'inline_post_engagement',
                        'video_avg_percent_watched_actions',
                        'outbound_clicks',
                        'unique_outbound_clicks',
                        'unique_clicks',
                        'inline_link_clicks',
                        'unique_inline_link_clicks',
                        'video_p25_watched_actions',
                        'video_p50_watched_actions',
                        'video_p75_watched_actions',
                        'video_p95_watched_actions',
                        'video_p100_watched_actions',
                        'video_10_sec_watched_actions',
                        'video_30_sec_watched_actions',
                        'frequency',
                        'reach',
                        'social_impressions',
                        'social_reach',
                        'actions',
                        'action_values',
                        'call_to_action_clicks',
                        'total_actions',
                        'total_unique_actions',
                        'unique_actions'
                        ]
    params['time_range'] = {'since': start_date.strftime('%Y-%m-%d'), 'until': end_date.strftime('%Y-%m-%d')}

    action_objects = ['actions',
                      'action_values',
                      'unique_actions',
                      'outbound_clicks',
                      'unique_outbound_clicks',
                      'cost_per_action_type',
                      'video_avg_percent_watched_actions',
                      'video_avg_time_watched_actions',
                      'video_p25_watched_actions',
                      'video_p50_watched_actions',
                      'video_p75_watched_actions',
                      'video_p95_watched_actions',
                      'video_p100_watched_actions',
                      'video_10_sec_watched_actions',
                      'video_30_sec_watched_actions'
                     ]

    report_values = {
                     'access_token': access_token,
                     'after': '',
                     'limit': 500
                    }

    i_async_job = {}
    i_async_job['report_run_id'] = '143585812939594'
    #i_async_job = acc.get_insights(params=params, async=True)
    print("Async job ID: %s" % i_async_job['report_run_id'])

    extract_incomplete = True

    retry_count = 0
    max_retries = 100

    row_count = 0
    page_number = 1


    while True:
        if retry_count > max_retries:
            print('max retries hit, aborting...')
            return

        retry_count += 1
        time.sleep(10)

        status = requests.get(STATUS_URL + i_async_job['report_run_id'], params= {'access_token': access_token}).json()
        print(status)
        print('async_percent_completion:' + str(status[AdReportRun.Field.async_percent_completion]))
        print ('job status: ' + status[AdReportRun.Field.async_status])


        if status[AdReportRun.Field.async_status] == "Job Completed":

            print ('report done, begin extract')

            while True:
                req = requests.get(STATUS_URL + i_async_job['report_run_id'] + "/insights",
                                   params=report_values)
                print('Request URL: %s', req.url)
                print('Status: %s' % req.status_code)

                if req.status_code == 200:

                    results = req.json()
                    for each_json in results['data']:
                        output_df = output_df.append(json_to_flatten(each_json))
                        row_count += 1
                    #import ipdb; ipdb.set_trace() # BREAKPOINT
                    #for line in results['data']:

                    #    # processes action objects to make it flatter
                    #    for idx in action_objects:
                    #        if idx in line:
                    #            line[idx + '_record'] = transform_action_array(line[idx])
                    #            # remove the original key
                    #            del line[idx]

                    #    row_count = row_count + 1


                    #    # write json to file followed by new line.

                    #    #if we want to do flattening processing in python, it goes <HERE>


                    if 'paging' in results and 'after' in results['paging']['cursors'] and results['paging']['cursors'][
                        'after'] != report_values['after']:
                        report_values['after'] = results['paging']['cursors']['after']
                        print('rows processed:' + str(row_count))
                        print('pages processed:' + str(page_number))
                    else:
                        print('rows processed:' + str(row_count))
                        print('pages processed:' + str(page_number))

                        return output_df

                    page_number += 1

