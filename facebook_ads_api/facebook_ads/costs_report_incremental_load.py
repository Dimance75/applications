import argparse
import psycopg2
import ConfigParser
import os

import datetime
from datetime import timedelta

from facebookads.api import FacebookAdsApi
from facebookads.adobjects import adaccount, adsinsights


def api_boot(credentials=None, app_id=None, app_secret=None, access_token=None):

    """Function initializing the API connection to Facebook Ads API. 
    Can take as parameters a dictionary with a section named facebookads containing the credentials OR directly variables from config file,
    in order to be able to get credentials directly from a yaml file OR from variables defined from config file data.
    In case all parameters are entered, the script will execute with credentials dictionary and ignore the variables."""

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

def get_costs(account_ids, since=(datetime.datetime.now() - timedelta(1)).strftime('%Y-%m-%d'), until=(datetime.datetime.now() - timedelta(1)).strftime('%Y-%m-%d')):

    """Function querying Facebook Insights API in order to get costs data at ad level, per region,
    for all account IDs in the list variable account_ids and appending data rows as lists into the costs_data list. 
    Default start date is yesterday, default end date is yesterday."""

    costs_data = []
    for ids in account_ids:
        accounts = adaccount.AdAccount(ids)
        report_fields = [
            adsinsights.AdsInsights.Field.account_name,
            adsinsights.AdsInsights.Field.account_id,
            adsinsights.AdsInsights.Field.campaign_name,
            adsinsights.AdsInsights.Field.adset_name,
            adsinsights.AdsInsights.Field.ad_name,
            adsinsights.AdsInsights.Field.impressions,
            adsinsights.AdsInsights.Field.clicks,
            adsinsights.AdsInsights.Field.spend
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
                               dataDict['account_id'].encode('utf-8'), dataDict['campaign_name'].encode('utf-8'),
                               dataDict['adset_name'].encode('utf-8'), dataDict['ad_name'].encode('utf-8'),
                               dataDict['impressions'].encode('utf-8'), dataDict['clicks'].encode('utf-8'),
                               dataDict['spend'].encode('utf-8')])

    print 'Facebook Ads Report successfully downloaded.'
    return costs_data

def pg_transfer(db_name, db_user, db_pw, db_host, data_list, starting_date):

    """Function connecting to Postgres, then deleting potentially existing data for the selected time range, then transferring the data previously collected."""

    with psycopg2.connect(database=db_name, user=db_user, password=db_pw, host=db_host) as connection_var:
        with connection_var.cursor() as cur:

            whereClause = str(""" date >= '%s'""") % starting_date

            cur.execute(
                """DELETE FROM dwh.facebookads_report WHERE %s""" % whereClause
            )

            print 'Transferring Facebook Ads Report to DWH.'

            for stored_lists in data_list:

                cur.execute(
                """INSERT INTO dwh.facebookads_report (date, region, ad_account, ad_id, campaign, ad_set, ad, impressions, clicks, spend)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                    (stored_lists[0], stored_lists[2], stored_lists[3], stored_lists[4], stored_lists[5], stored_lists[6], stored_lists[7], stored_lists[8], stored_lists[9], stored_lists[10])
                )

    print 'Facebook Ads Report successfully transferred to DWH.'

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Pull Data From Facebook API Into Postgres')
    parser.add_argument('config_file_name', help='Absolute path to config-file')
    args = parser.parse_args()

    config = ConfigParser.ConfigParser()
    config.read(args.config_file_name)

    section = 'fb'
    fb_settings = config.items(section)
    cred_dict = dict()
    for k,v in fb_settings: cred_dict[k] = v if v != '' else None
    cred_dict['acc_ids'] = [x.strip() for x in cred_dict['acc_ids'].split(',')]

    section = 'db'
    dwh_settings = config.items(section)
    db_dict = dict()
    for k,v in dwh_settings: db_dict[k] = v if v != '' else None

    api_boot(app_id=cred_dict['app_id'], app_secret=cred_dict['app_secret'], access_token=cred_dict['system_user_token'])
    costs_lists = get_costs(account_ids=cred_dict['acc_ids'])

    pg_transfer(db_dict['dbname'],
                db_dict['user'],
                db_dict['password'],
                db_dict['host'],
                costs_lists,
                (datetime.datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
                )