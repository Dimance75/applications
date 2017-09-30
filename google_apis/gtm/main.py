import argparse

from gtm_objects import *
from utils import *
from helpers.guide_parser import *

from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client import client
from oauth2client import file
from oauth2client import tools

def main(custom_vars_path):

    with open(custom_vars_path, 'r') as stream:
        config_dict = yaml.load(stream)

    # Parse authentication variables and parameters from config file.
    scope = config_dict['AUTHENTICATION']['SCOPE']
    GTM_ACCOUNT_ID = config_dict['AUTHENTICATION']['GTM_ACCOUNT_ID']
    GTM_SERVICE_ACCOUNT_PATH = config_dict['AUTHENTICATION']['GTM_SERVICE_ACCOUNT_PATH']
    GA_TRACKING_ID = config_dict['AUTHENTICATION']['GA_TRACKING_ID']
    TEST_CONTAINER_ID = config_dict['LOCATION']['TEST_CONTAINER_ID']
    WORKSPACE_ID = config_dict['LOCATION']['WORKSPACE_ID']
    CUSTOM_DIMENSIONS = config_dict['CUSTOM_DIMENSIONS']

    GA_SETTINGS_VARNAME = config_dict['SETTINGS']['GA_SETTINGS_VARIABLE_NAME']

    FOLDER_NAME = config_dict['LOCATION']['PANDATA_FOLDER_NAME']

    IMPLEM_GUIDE_CLASSIC_PATH = config_dict['IMPLEMENTATION_GUIDE']['PATH']
    VIRTUAL_PAGEVIEWS = config_dict['SETTINGS']['VIRTUAL_PAGEVIEWS']
    VIRTUAL_PAGEVIEWS_EVENT_NAME = config_dict['SETTINGS']['VIRTUAL_PAGES_EVENTS']

    WORKSPACE_NAME = 'ga_tags_pandata'

    creds = get_credentials(service_account_path=GTM_SERVICE_ACCOUNT_PATH, scope=scope)

    http = creds.authorize(http=httplib2.Http())

    # Build the service object.
    service = build('tagmanager', 'v2', http=http)

    container = get_container(service, GTM_ACCOUNT_ID, TEST_CONTAINER_ID)

    workspace = get_workspace_byname(service, container, WORKSPACE_NAME)

    events_dict = parse_implementation_guide_classic(IMPLEM_GUIDE_CLASSIC_PATH)

    if get_folder_byname(service, workspace, FOLDER_NAME) is not None:
        autodelete_folder_entities(service, workspace, FOLDER_NAME)
    else:
        create_folder(service, workspace, FOLDER_NAME)

    autocreate_dl_variables(service, workspace, events_dict, FOLDER_NAME)
    autocreate_triggers(service, workspace, events_dict, FOLDER_NAME)
    autocreate_tags(service, workspace, events_dict, FOLDER_NAME)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run basic GTM Setup Script')
    parser.add_argument('config_file_name', help='absolute path to config-file')
    args = parser.parse_args()
    main(args.config_file_name)
