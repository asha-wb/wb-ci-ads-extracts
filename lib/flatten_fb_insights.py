import json
import pandas as pd
from pandas.io.json import json_normalize
from flatten_json import flatten


COLS_ORDER = ['date_start', 'date_stop', 'ad_id', 'ad_name', 'adset_id',
              'adset_name', 'account_id', 'account_name', 'campaign_id',
              'campaign_name', 'age', 'gender', 'video_avg_time_watched_actions',
              'impressions', 'spend', 'page_engagement', 'like', 'post_engagement',
              'post', 'comment', 'post_reaction', 'video_avg_percent_watched_actions',
              'outbound_clicks', 'unique_outbound_clicks', 'clicks', 'link_click',
              'unique_inline_link_clicks', 'video_p25_watched_actions',
              'video_p50_watched_actions', 'video_p75_watched_actions',
              'video_p95_watched_actions', 'video_p100_watched_actions',
              'video_10_sec_watched_actions', 'video_30_sec_watched_actions',
              'frequency', 'reach', 'social_impressions', 'social_reach',
              'call_to_action_clicks'
             ]

value_getter = {
        'date_start'                            : lambda item: item['date_start'],
        'date_stop'                             : lambda item: item['date_stop'],
        'ad_id'                                 : lambda item: item['ad_id'],
        'ad_name'                               : lambda item: item['ad_name'],
        'adset_id'                              : lambda item: item['adset_id'],
        'adset_name'                            : lambda item: item['adset_name'],
        'account_id'                            : lambda item: item['account_id'],
        'account_name'                          : lambda item: item['account_name'],
        'campaign_id'                           : lambda item: item['campaign_id'],
        'campaign_name'                         : lambda item: item['campaign_name'],
        'age'                                   : lambda item: item['age'],
        'gender'                                : lambda item: item['gender'],
        'video_avg_time_watched_actions'        : lambda item: item['video_avg_time_watched_actions'][0]['value'],
        'impressions'                           : lambda item: item['impressions'],
        'spend'                                 : lambda item: item['spend'],
        'page_engagement'                       : lambda item: item['page_engagement'],

        }


sample_output1 = {
  "account_id": "186520528512505",
  "account_name": "Stage13AdAccount",
  "actions": [
    {
      "action_type": "video_view",
      "value": "1"
    },
    {
      "action_type": "page_engagement",
      "value": "1"
    },
    {
      "action_type": "post_engagement",
      "value": "1"
    }
  ],
  "ad_id": "23842694420500058",
  "ad_name": "Episode 4",
  "adset_id": "23842694420450058",
  "adset_name": "3P Gamer Affinities - Episode 4",
  "age": "35-44",
  "campaign_id": "23842688125980058",
  "campaign_name": "Independent Launch 10.19.17 - Post Views",
  "date_start": "2017-10-24",
  "date_stop": "2017-10-24",
  "frequency": "1",
  "gender": "unknown",
  "impressions": "4",
  "inline_link_clicks": "0",
  "inline_post_engagement": "0",
  "reach": "4",
  "social_impressions": "0",
  "social_reach": "0",
  "spend": "0",
  "total_actions": "1",
  "total_unique_actions": "1",
  "unique_actions": [
    {
      "action_type": "page_engagement",
      "value": "1"
    },
    {
      "action_type": "post_engagement",
      "value": "1"
    },
    {
      "action_type": "video_view",
      "value": "1"
    }
  ],
  "unique_clicks": "0",
  "unique_inline_link_clicks": "0",
  "video_avg_percent_watched_actions": [
    {
      "action_type": "video_view",
      "value": "0.35"
    }
  ],
  "video_avg_time_watched_actions": [
    {
      "action_type": "video_view",
      "value": "2"
    }
  ]
  }

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

sample_output2 = {
  "account_id": "186520528512505",
  "account_name": "Stage13AdAccount",
  "actions": [
    {
      "action_type": "link_click",
      "value": "1"
    },
    {
      "action_type": "video_view",
      "value": "42"
    },
    {
      "action_type": "page_engagement",
      "value": "43"
    },
    {
      "action_type": "post_engagement",
      "value": "43"
    },
    {
      "action_type": "abdc234",
      "value": "200"
    }
  ],
  "ad_id": "23842680939450058",
  "ad_name": ":15 Series Teaser (No Date)",
  "adset_id": "23842680870150058",
  "adset_name": "3P Horror Genre Rev",
  "age": "18-24",
  "campaign_id": "23842680869910058",
  "campaign_name": "2SHS- Go90 (Rev)",
  "clicks": "1",
  "date_start": "2017-09-15",
  "date_stop": "2017-10-15",
  "frequency": "1.007353",
  "gender": "female",
  "impressions": "137",
  "inline_link_clicks": "1",
  "outbound_clicks": [
    {
      "action_type": "outbound_click",
      "value": "1"
    }
  ],
  "reach": "136",
  "social_impressions": "0",
  "social_reach": "0",
  "spend": "0.96",
  "unique_inline_link_clicks": "1",
  "unique_outbound_clicks": [
    {
      "action_type": "outbound_click",
      "value": "1"
    }
  ],
  "video_10_sec_watched_actions": [
    {
      "action_type": "video_view",
      "value": "19"
    }
  ],
  "video_avg_percent_watched_actions": [
    {
      "action_type": "video_view",
      "value": "41.01"
    }
  ],
  "video_avg_time_watched_actions": [
    {
      "action_type": "video_view",
      "value": "6"
    }
  ],
  "video_p100_watched_actions": [
    {
      "action_type": "video_view",
      "value": "12"
    }
  ]
}


def flatten_json(data):


    return


def json_to_flatten(adsData):
    #dict_train = json.load(adsData)
    flatten_dict = flatten(adsData)
    for k in flatten_dict.keys():
        if k.startswith('actions_'):
            flatten_dict.pop(k)
    df = pd.DataFrame(flatten_dict , index=[0])
    df = df[df.columns.drop(list(df.filter(regex='_action_type')))]
    try:
        data = pd.DataFrame(adsData['actions'])
    except KeyError:
        final_set_of_data = df
    else:
        action_data = data.set_index('action_type').T
        action_data.reset_index(drop=True, inplace=True)
        final_set_of_data = pd.concat([df,action_data], axis=1,ignore_index=False)

    final_set_of_data = final_set_of_data.reindex(columns = ['date_start', 'date_stop', 'ad_id', 'ad_name', 'adset_id','adset_name', 'account_id', 'account_name', 'campaign_id','campaign_name', 'age', 'gender', 'video_avg_time_watched_actions_0_value','impressions', 'spend', 'page_engagement', 'like', 'post_engagement','post', 'comment', 'post_reaction', 'video_avg_percent_watched_actions_0_value','outbound_clicks_0_value', 'unique_outbound_clicks_0_value', 'clicks', 'link_click','unique_inline_link_clicks', 'video_p25_watched_actions_0_value','video_p50_watched_actions_0_value', 'video_p75_watched_actions_0_value','video_p95_watched_actions_0_value', 'video_p100_watched_actions_0_value','video_p10_watched_actions_0_value', 'video_30_sec_watched_actions_0_value', 'frequency', 'reach', 'social_impressions', 'social_reach','call_to_action_clicks'])

    return final_set_of_data
