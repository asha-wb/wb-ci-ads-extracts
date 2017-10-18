
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
    params['fields'] = ['account_id', 'account_name', 'ad_id', 'ad_name', 'adset_id',
                        'adset_name', 'campaign_id', 'campaign_name',
                        'call_to_action_clicks', 'inline_link_clicks', 'unique_clicks', 'frequency',
                        'impressions',
                        'reach', 'relevance_score', 'social_clicks', 'unique_social_clicks',
                        'social_impressions', 'social_reach', 'spend', 'action_values', 'actions',
                        'cost_per_action_type', 'cost_per_total_action', 'total_action_value',
                        'total_actions', 'total_unique_actions', 'unique_actions']
    params['time_range'] = {'since': '2016-07-05', 'until': '2017-10-05'}

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

                        # if you want to do row wise operations, do it here and write this to file instead.
                        # print(line)
                        row_count = row_count + 1

                    # write data to file as raw json
                    # json.dump(results['data'], outfile)
                    json.dump(results, outfile)



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



