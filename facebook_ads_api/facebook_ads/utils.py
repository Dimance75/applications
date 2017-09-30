#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import datetime
from datetime import timedelta
from facebookads.api import FacebookAdsApi
from facebookads.adobjects import adsinsights, adaccount


def api_boot(credentials=None, app_id=None, app_secret=None, access_token=None):

    """Function initializing the API connection to Facebook Ads API. 
    Can take as parameters a dictionary with a section named facebookads containing the credentials OR directly variables from config file,
    in order to be able to get credentials directly from a yaml file OR from variables defined from config file data.
    In case all parameters are entered, the script will execute with credentials dictionary and ignore the variables."""

    try:

        assert credentials is not None or (app_id is not None and app_secret is not None and access_token is not None)

        if credentials is not None:
            application_id = credentials['facebook_ads']['app_id']
            secret = credentials['facebook_ads']['app_secret']
            token = credentials['facebook_ads']['access_token']
        else:
            application_id = app_id
            secret = app_secret
            token = access_token

        FacebookAdsApi.init(application_id, secret, token)
        print("Facebook Ads API successfully initialized.")

    except Exception as ex:
        print("Error during Facebook Ads API initialization:\n" + ex + '\n' + type(ex) + '\n' + ex.args)


def get_costs(account_ids, since=str(datetime.datetime.now() - timedelta(1)).strftime('%Y-%m-%d'), until=str(datetime.date.today())):

    """Function querying Facebook Insights API in order to get costs data at ad level, per region,
    for all account IDs in the list variable account_ids and appending data rows as lists into the costs_data list. 
    Default start date is yesterday, default end date is current day."""

    try:

        costs_data = []
        for ids in account_ids:
            accounts = adaccount(ids)
            report_fields = [
                adsinsights.Field.account_name,
                adsinsights.Field.campaign_name,
                adsinsights.Field.adset_name,
                adsinsights.Field.ad_name,
                adsinsights.Field.spend
            ]

            params = {'time_range': {'since': since, 'until': until},
                      'level': 'ad',
                      'breakdowns': ['region'],
                      'time_increment': 1
                      }

            insights = accounts.get_insights(fields=report_fields, params=params)
            # Querying the API on the defined fields and parameters

            for dataDict in insights:  # For all data dictionaries in the api response (= insights)
                costs_data.append([dataDict['date_start'].encode('utf-8'), dataDict['date_stop'].encode('utf-8'),
                                   dataDict['region'].encode('utf-8'), dataDict['account_name'].encode('utf-8'),
                                   dataDict['campaign_name'].encode('utf-8'), dataDict['adset_name'].encode('utf-8'),
                                   dataDict['ad_name'].encode('utf-8'), dataDict['spend']])

        return costs_data

    except Exception as ex:
        print("Error during Facebook costs report creation:\n" + ex + '\n' + type(ex) + '\n' + ex.args)
