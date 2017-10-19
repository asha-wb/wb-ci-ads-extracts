
from facebookads import FacebookSession
from facebookads import FacebookAdsApi
from facebookads.adobjects.adaccountuser import AdAccountUser
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.adreportrun import AdReportRun

from facebookads.adobjects.campaign import Campaign


import json
import os
import pprint
import requests
import time

STATUS_URL = "https://graph.facebook.com/v2.10/"

pp = pprint.PrettyPrinter(indent=4)


def transform_action_array(arr):
    """ Transform an action or action_value array into a record structure. """

    ret = {}
    for row in arr:
        ret[row['action_type']] = row['value']

    return ret


def get_insights():
    print('Starting get insights')

    this_dir = os.path.dirname(__file__)
    config_filename = os.path.join(this_dir, 'config.json')

    config_file = open(config_filename)
    config = json.load(config_file)
    config_file.close()

    ### Setup session and api objects
    session = FacebookSession(
        config['app_id'],
        config['app_secret'],
        config['access_token'],
    )
    api = FacebookAdsApi(session)


    FacebookAdsApi.set_default_api(api)


    ad_account_id = 'act_186520528512505'

    acc = AdAccount(ad_account_id)
    print(acc.get_id())

    params = dict()
    params['access_token'] = config['access_token']
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
                        'unique_actions']
    params['time_range'] = {'since': '2017-09-05', 'until': '2017-10-05'}

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
        'access_token': config['access_token']
        , 'after': '',
        'limit': 500
    }

    acc = AdAccount(ad_account_id)
    i_async_job = acc.get_insights(params=params, async=True)
    print("Async job ID: %s" % i_async_job['report_run_id'])

    extract_incomplete = True


    retry_count = 0
    max_retries = 100

    row_count = 0
    page_number = 0

    # print(config['access_token'])

    while True:
        if retry_count > max_retries:
            print('max retries hit, aborting')

        time.sleep(10)

        status = requests.get(STATUS_URL + i_async_job['report_run_id'], params= {'access_token': config['access_token']}).json()
        print(status)
        print('async_percent_completion:' + str(status[AdReportRun.Field.async_percent_completion]))
        print ('job status: ' + status[AdReportRun.Field.async_status])


        if status[AdReportRun.Field.async_status] == "Job Completed":

            print ('report done, begin extract')

            outfile = open('outfile.json', 'w+')

            while True:
                req = requests.get(STATUS_URL + i_async_job['report_run_id'] + "/insights",
                                   params=report_values)
                print('Request URL: %s', req.url)
                print('Status: %s' % req.status_code)

                if req.status_code == 200:

                    results = req.json()

                    for line in results['data']:

                        # processes action objects to make it flatter
                        for idx in action_objects:
                            if idx in line:
                                line[idx + '_record'] = transform_action_array(line[idx])
                                # remove the original key
                                del line[idx]

                        row_count = row_count + 1


                        # write json to file followed by new line.
                        json.dump(line, outfile)
                        outfile.write('\n')

                    if 'paging' in results and 'after' in results['paging']['cursors'] and results['paging']['cursors'][
                        'after'] != report_values['after']:
                        report_values['after'] = results['paging']['cursors']['after']
                    else:
                        print('rows processed:' + str(row_count))
                        print('pages processed:' + str(page_number))
                        outfile.close()

                        return

                    page_number += 1


if __name__ == '__main__':
    print('starting')

    get_insights()

    print('done')



