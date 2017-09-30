import os
import uuid
import time

from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

SCOPES = ['https://www.googleapis.com/auth/bigquery']
CLIENT_SECRETS = 'sa_credentials.json'

def async_query(bigquery, project_id, query, batch=False, num_retries=5):
    # Generate a unique job_id so retries
    # don't accidentally duplicate query
    job_data = {
        'jobReference': {
            'projectId': project_id,
            'job_id': str(uuid.uuid4())
        },
        'configuration': {
            'query': {
                'query': query,
                'priority': 'BATCH' if batch else 'INTERACTIVE'
            }
        }
    }
    return bigquery.jobs().insert(
        projectId=project_id,
        body=job_data).execute(num_retries=num_retries)

def poll_job(bigquery, job):
    """Waits for a job to complete."""

    print('Waiting for job to finish...')

    request = bigquery.jobs().get(
        projectId=job['jobReference']['projectId'],
        jobId=job['jobReference']['jobId'])

    while True:
        result = request.execute(num_retries=2)

        if result['status']['state'] == 'DONE':
            if 'errorResult' in result['status']:
                raise RuntimeError(result['status']['errorResult'])
            print('Job complete.')
            return

        time.sleep(1)

def query_bigquery(project_id, query):
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   CLIENT_SECRETS)
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_path, scopes=SCOPES)

    # Create a BigQuery client using the credentials.
    bigquery_service = discovery.build(
        'bigquery', 'v2', credentials=credentials)

    query_job = async_query(bigquery_service, project_id, query)

    poll_job(bigquery_service, query_job)

    page_token = None
    query_response = []

    print 'Getting query results...'
    while True:
        page = bigquery_service.jobs().getQueryResults(
            pageToken=page_token,
            **query_job['jobReference']).execute(num_retries=2)

        for row in page['rows']:
            temp = []
            for field in row['f']:
                temp.append(field['v'])
            query_response.append(temp)

        page_token = page.get('pageToken')
        if not page_token:
            break

    return query_response