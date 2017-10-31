# -*- coding: utf-8 -*-
#lib/token_utils.py

import requests
import logging


logger = logging.getLogger(__name__)


def get_extended_access_token(app_id, app_secret, old_token):
    params = {'grant_type': 'fb_exchange_token', 'client_id': app_id, 'client_secret': app_secret, 'fb_exchange_token': old_token}
    logger.info("getting new access token")
    resp = requests.get('https://graph.facebook.com/oauth/access_token?', params=params)
    if resp.status_code != 200:
        logger.error("Failed to request for new access token... %s" % (resp.raise_for_status()))
        raise

    resp = resp.json()
    return resp['access_token']
