# -*- coding: utf-8 -*-

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.errors import HttpError
from apiclient import discovery
from time import sleep
from datetime import date, datetime, timedelta
from utils.utils import perdelta

import os
import httplib2
import json
import ConfigParser
import argparse
import psycopg2

SQL_CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS {schema};"
SQL_CREATE_TABLE = "CREATE TABLE IF NOT EXISTS {schema}.{table} (date DATE, data JSON);"
SQL_INSERT = "INSERT INTO {schema}.{table} VALUES (%(date)s, %(row)s);"
SQL_TRUNCATE_TABLE = "TRUNCATE {schema}.{table};"
SQL_DELETE_WHERE_DATE = "DELETE FROM {schema}.{table} WHERE date = %s;"


def create_objectlayer_query(report, dl_schema, ol_schema, table):
    """
    :param report: Google Analytics API report body (for dimensions and metrics)
    :param dl_schema: schema where the data is received from
    :param ol_schema: schema where the data is going to
    :param table: name of table; same name for both schemata
    :return: Query that would drop an existing table and
            create a new one with all the data from the Google Analytics API Report.
            All Columns are type TEXT
    """
    dimensions = [dim['name'].split(':')[1] for dim in report['reportRequests'][0]['dimensions']]
    metrics = [metric['expression'].split(':')[1] for metric in report['reportRequests'][0]['metrics']]
    query = '''
    DROP TABLE IF EXISTS {ol_schema}.{table};
    CREATE TABLE {ol_schema}.{table} AS
    SELECT
        {dimensions},
        {metrics}
    FROM {dl_schema}.{table};
    '''
    dimension_strings = ("data -> 'dimensions' ->> " + str(key) + " AS " + str(value) for key, value in
                         dict(enumerate(dimensions)).items())
    metric_strings = ("(data -> 'metrics' -> 0 -> 'values' ->> " + str(key) + "):: NUMERIC AS " + str(value)
                      for key, value in dict(enumerate(metrics)).items())

    return query.format(ol_schema=ol_schema, table=table, dl_schema=dl_schema,
                        dimensions=',\n'.join(dimension_strings),
                        metrics=',\n'.join(metric_strings))


def run(pg_db, pg_cur, config_dict, debug=False):
    """
    Takes a given Google Analytics report body and pulls the data from the Google Analytics API,
    pushes the data into the database and creates extraction sql's to extract the data from the json response.
    All fields will be type 'TEXT'.
    :param pg_db: psycopg2 database connection
    :param pg_cur: psycopg2 connection cursor
    :param config_dict: dictionary with following keys:
                key_file_location: location of service account credentials - json-file
                token_uri: e.g. https://www.googleapis.com/auth/analytics.readonly
                service_url: e.g. https://analyticsreporting.googleapis.com/$discovery/rest
                view_id: Google Analytics view (profile) ID
                day_one: first day that has data in Google Analytics; Defaults to 2017-01-01
                is_full_load: 0 or 1; whether all data should be loaded (from day_one);
                    will delete all existing data for the given reports
                start_date: first day that should be pulled; If not given defaults to 'yesterday'
                end_date: last day that should be pulled; If not given defaults to 'yesterday'
                report_location: Directory where the report files are stored
                report_file_name_format: format of the file names; must contain '{0}' exactly once.
                    If not given defaults to '{0}.json'
                reports: names of the reports as comma separated list e.g. 'adcosts, sessions'
                objectlayer_query_location: location where the .sql should be saved
                objectlayer_query_format: format for file names of .sql files; defaults to 'db_table_prefix + {0].sql'
                db_datalayer_schema: schema where the datalayer tables should be stored
                db_objectlayer_schema: schema where the .sql would create the tables
                db_table_prefix: prefix for all tables e.g. ga_
    :param debug: Boolean for debug mode; nothing will be committed
    :return:
    """
    # auth
    key_file_location = config_dict.get("key_file_location")
    token_uri = config_dict.get("token_uri")  # https://www.googleapis.com/auth/analytics.readonly
    service_url = config_dict.get("service_url")  # https://analyticsreporting.googleapis.com/$discovery/rest

    # reports
    view_id = config_dict.get('view_id')
    day_one_str = config_dict.get('day_one')
    is_full_load = bool(int(config_dict.get('is_full_load')))
    start_date_str = config_dict.get("start_date")
    end_date_str = config_dict.get("end_date")
    report_location = config_dict.get("report_location")
    report_file_name_format = config_dict.get("report_file_name_format", '{0}.json')
    reports = [x.strip() for x in config_dict.get("reports", '').split(',')]

    # ol query
    ol_query_location = config_dict.get('objectlayer_query_location')
    ol_query_file_name_format = config_dict.get('objectlayer_query_format',
                                                config_dict.get("db_table_prefix", '') + '{0}.sql')

    # database
    db_datalayer_schema = config_dict.get("db_datalayer_schema")
    db_objectlayer_schema = config_dict.get("db_objectlayer_schema")
    db_table_prefix = config_dict.get("db_table_prefix", '')

    if debug:
        print ol_query_file_name_format
        for key, value in config_dict.items():
            print key, ' ', value
        print report_file_name_format
        for report in reports:
            print os.path.join(report_location, report_file_name_format.format(report))
            # return

    # create service credentials
    credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location, [token_uri])

    # create service object to create reports
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('analytics', 'v4', http=http,
                              discoveryServiceUrl=service_url)

    # if config.get(section, "full_load") == '1':
    #     start_date_str = '2014-01-01'
    # else:
    #     start_date_str = 'yesterday'

    pg_cur.execute(SQL_CREATE_SCHEMA.format(schema=db_datalayer_schema))

    for report in reports:
        report_file_name = os.path.join(report_location, report_file_name_format.format(report))
        ol_query_file_name = os.path.join(ol_query_location, ol_query_file_name_format.format(report))
        table_name = db_table_prefix + report

        pg_cur.execute(SQL_CREATE_TABLE.format(schema=db_datalayer_schema, table=table_name))

        with open(report_file_name) as json_data:
            report_body = json.load(json_data)

            ol_query = create_objectlayer_query(report_body, db_datalayer_schema, db_objectlayer_schema, table_name)

        with open(ol_query_file_name, 'w+') as ol_query_sql:
            ol_query_sql.write(ol_query)

        default_date = date.today() - timedelta(days=1)
        if is_full_load:
            start_date = datetime.strptime(day_one_str, '%Y-%m-%d').date() if day_one_str else date(2017, 01, 01)
            end_date = default_date
            pg_cur.execute(SQL_TRUNCATE_TABLE.format(schema=db_datalayer_schema, table=table_name))
        else:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else default_date
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else default_date

        for day in perdelta(start_date, end_date, timedelta(days=1)):
            page_token = 0
            row_count = 1

            sql = SQL_DELETE_WHERE_DATE.format(schema=db_datalayer_schema, table=table_name)
            pg_cur.execute(sql, (day,))
            timer = datetime.now()

            while int(page_token) < row_count:
                report_body['reportRequests'][0]['viewId'] = view_id
                report_body['reportRequests'][0]['pageSize'] = 10000
                report_body['reportRequests'][0]['pageToken'] = '{0}'.format(page_token)
                report_body['reportRequests'][0]['dateRanges'][0]['startDate'] = str(day)
                report_body['reportRequests'][0]['dateRanges'][0]['endDate'] = str(day)

                print report
                print report_body
                # report body with {view_id} and {page_token}

                if datetime.now() - timer < timedelta(seconds=1):
                    sleep(1)
                timer = datetime.now()
                retries = 0
                while True:
                    try:
                        response = service.reports().batchGet(body=report_body).execute()
                        break
                    except HttpError as e:
                        print 'Received error response', e.message
                        print 'Retrying in 10 seconds'
                        if retries > 2:
                            raise e
                        retries += 1
                        sleep(10)

                response_part = response['reports'][0]

                print 'nextPageToken', response_part.get('nextPageToken', '0')

                row_count = response_part['data'].get('rowCount', 0)
                print 'rowcount', row_count

                sqls = list()
                for row in response_part['data'].get('rows', {}):
                    row_json = json.dumps(row)
                    sql = SQL_INSERT.format(schema=db_datalayer_schema, table=db_table_prefix + report)
                    sqls.append(pg_cur.mogrify(sql, {'date': day, 'row': row_json}))
                if sqls:
                    pg_cur.execute(''.join(sqls))

                if 'nextPageToken' in response_part:
                    page_token = response_part.get('nextPageToken')
                else:
                    break

            if not debug:
                print 'Executed. Committing...'
                pg_db.commit()
                print 'Committed.'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pull Data From GA API Into Postgres')
    parser.add_argument('config_file_name', help='absolute path to config-file')
    parser.add_argument('--debug', dest='debug', action='store_true')
    parser.set_defaults(debug=False)

    args = parser.parse_args()
    debug = args.debug

    config = ConfigParser.ConfigParser()
    config.read(args.config_file_name)

    section = 'db'
    pg_db = psycopg2.connect(host=config.get(section, "host"),
                             dbname=config.get(section, "dbname"),
                             user=config.get(section, "user"),
                             password=config.get(section, "password"))
    section = 'ga'
    options = config.items(section)
    config_dict = dict()
    for key, value in options:
        if value != '':
            config_dict[key] = value

    with pg_db:
        with pg_db.cursor()as pg_cur:
            run(pg_db, pg_cur, config_dict, debug)
            if debug:
                pg_db.rollback()

    pg_db.close()
