import psycopg2
import configparser
import argparse
import time
import uuid
import csv
import tinys3
import datetime
import os
from datetime import timedelta
from google.cloud import bigquery

csv_path = os.path.expanduser("~") + '/roi_report/bigquery_loader/csv_files/'
if not os.path.isdir(csv_path):
    os.makedirs(csv_path)
CSV_FOLDER = csv_path


def wait_for_job(job):

    while True:
        job.reload()  # Refreshes the state via a GET request.
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)


def async_query(bq_client, query):

    try:
        client = bq_client
        stored_rows = []
        query_job = client.run_async_query(str(uuid.uuid4()), query)
        query_job.use_legacy_sql = False
        query_job.begin()

        wait_for_job(query_job)

        # Drain the query results by requesting a page at a time.
        query_results = query_job.results()
        page_token = None

        while True:
            rows, total_rows, page_token = query_results.fetch_data(
                page_token=page_token)
            for row in rows:
                stored_rows.append(list(row))

            if not page_token:
                return stored_rows
    except Exception as ex:
        print('Error during async_query:\n%s \n%s \n%s') % (type(ex), ex.args, ex)


def get_data_bq(project, query):

    try:

        bigquery_client = bigquery.Client.from_service_account_json(json_credentials_path=CREDENTIALS, project=project)
        print("Successfully created BQ Client, running query.")
        bigquery_data = async_query(bigquery_client, query)

        return bigquery_data

    except Exception as ex:
        print('Error while querying Google BigQuery:\n%s \n%s \n%s') % (type(ex), ex.args, ex)


def create_csv(data_list, csv_name):

    try:
        csv_title = CSV_FOLDER + "%s.csv" % csv_name
        row_count = 0
        with open(csv_title, "w") as csvDump:
            wr = csv.writer(csvDump, quoting=csv.QUOTE_ALL)
            for rows in data_list:
                wr.writerow(rows)
                row_count += 1

        print("CSV file successfully created, {0} rows processed.".format(row_count))

    except Exception as ex:
        print('Error during CSV file creation:\n%s \n%s \n%s') % (type(ex), ex.args, ex)

def upload_csv_to_s3(s3_key, s3_secret, s3_endpoint, s3_bucket, local_csv_name, filetitle_s3):

    success = False
    tries = 0
    file_path = CSV_FOLDER + "%s.csv" % local_csv_name

    print('Uploading CSV file to S3...')

    while not success:
        tries += 1
        try:
            conn = tinys3.Connection(s3_key,
                                     s3_secret,
                                     tls=True,
                                     endpoint=s3_endpoint)

            file_to_upload = open('{0}'.format(file_path), 'rb')
            print('{0}'.format(filetitle_s3))
            print('{0}\n{1}\n{2}'.format(filetitle_s3,
                                         file_to_upload,
                                         s3_bucket))
            conn.upload(filetitle_s3,
                        file_to_upload,
                        s3_bucket)
            print('Successfully uploaded CSV file to S3.')
            success = True
        except Exception as ex:
            print(ex)
            if tries >= 5:
                print('5 tries were unsuccessful, breaking process.')
                break
            else:
                print('Connection error. Trying again..')


def csv_s3_to_rs(csv_full_path_s3, s3_key, s3_secret, rs_name, rs_port, rs_user, rs_pwd, rs_host, rs_table):
    try:

        # Connect to RedShift
        conn_string = "dbname=%s port=%s user=%s password=%s host=%s" % (rs_name, rs_port, rs_user, rs_pwd, rs_host)
        conn = psycopg2.connect(conn_string)
        print("Connection to Redshift successful.")

        # Create the cursor object.
        cursor = conn.cursor()

        copy_query = """
        COPY %s
        from '%s'
        CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'
        DELIMITER AS ','
        ;
        """ % (rs_table, csv_full_path_s3, s3_key, s3_secret)

        # Execute this COPY the file stored in S3 bucket to Redshift.
        print("Running COPY query...")
        cursor.execute(copy_query)
        print("COPY query worked successfully.")

    except Exception as inst:
        print(inst)
        conn.rollback()
        print("Error when COPYing data to Redshift, rolled back changes.")

    finally:
        conn.commit()
        conn.close()
        print("Connection to Redshift closed.")
        print("Execution over.")


if __name__ == '__main__':
    # Loading credentials from config file.
    parser = argparse.ArgumentParser(description='Get Credentials')
    parser.add_argument('config_file_name', help='absolute path to config-file')
    parser.add_argument('--debug', dest='debug', action='store_true')
    parser.set_defaults(debug=False)

    args = parser.parse_args()
    debug = args.debug

    config = configparser.ConfigParser()
    config.read(args.config_file_name)

    section = 'db'
    HOST = config.get(section, "host")
    DBNAME = config.get(section, "dbname")
    USER = config.get(section, "user")
    PWD = config.get(section, "password")
    PORT = config.get(section, "port")

    section = 'aws'
    S3_ACCESS_KEY = config.get(section, "s3_access_key")
    S3_SECRET_KEY = config.get(section, "s3_secret_key")
    S3_ENDPOINT = config.get(section, "s3_endpoint")
    S3_BUCKET = config.get(section, "s3_bucket")

    section = 'bigquery'
    CREDENTIALS = config.get(section, "credentials_path_dev")
    PROJECT = config.get(section, "project_name")

    bq_query = 'User-defined SQL Query.'
    create_csv_name = 'User-defined CSV name.'
    rs_target_table = 'User-defined schema.table.'

    bq_data = get_data_bq(project=PROJECT, query=bq_query)

    create_csv(bq_data, create_csv_name)

    upload_csv_to_s3(s3_key=S3_ACCESS_KEY, s3_secret=S3_SECRET_KEY, s3_endpoint=S3_ENDPOINT, s3_bucket=S3_BUCKET,
                     local_csv_name=create_csv_name, filetitle_s3=create_csv_name)

    csv_s3_to_rs(csv_full_path_s3=S3_BUCKET + create_csv_name, s3_key=S3_ACCESS_KEY, s3_secret=S3_SECRET_KEY,
                 rs_name=DBNAME, rs_port=PORT, rs_user=USER, rs_pwd=PWD, rs_host=HOST, rs_table=rs_target_table)
