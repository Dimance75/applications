import httplib2
import os
import itertools


from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client import client
from oauth2client import file
from oauth2client import tools

from gtm_objects.triggers import EzTrigger
from gtm_objects.tags import EzTag
from gtm_objects.variables import EzVar
from helpers.guide_parser import *

##########################################################################
#                       Basic utils                                      #
##########################################################################

def get_recursively(search_dict, field):

    """
    Takes a dict with nested lists and dicts,
    and searches all dicts for a key of the field
    provided.
    
    Args:
        search_dict: the dictionary object the function will look into
        field: the key for which we want to return the value(s)
        
    Returns:
        fields_found: a list object containing the value(s) for the searched key in the dictionary.
    """

    fields_found = []

    for key, value in search_dict.iteritems():

        if key == field:
            fields_found.append(value)

        elif isinstance(value, dict):
            results = get_recursively(value, field)
            for result in results:
                fields_found.append(result)

        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    more_results = get_recursively(item, field)
                    for another_result in more_results:
                        fields_found.append(another_result)

    return fields_found

def events_regex(event_keys):
    """
    Builds a simple "OR" RegEx based on a list of event keys.

    Args:
        event_keys: the list of events keys

    Returns:
        regex: a string of the following format: 'eventk1|eventk2|eventk3|...|eventkN'.
    """

    regex = ''
    if len(event_keys) > 1:
        for i, e in enumerate(event_keys):
            if i != len(event_keys) - 1:
                regex += str(e.replace(" ", "") + '|')
            else: regex += str(e)
    elif len(event_keys) == 1:
        regex += str(event_keys[0])

    return regex

##########################################################################
#                       GET utils                                        #
##########################################################################


def get_credentials(service_account_path, scope):

    """Get credentials for Google API access.
    Args: 
        service_account_path: string Path to the service account file.
        scope: the scope of authorization for the selected API.
    
    Returns:
        credentials: The credentials to used for the API.
    """

    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = service_account_path
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_path, scopes=scope)
    return credentials


def get_service(api_name, api_version, scope, client_secrets_path):

    """Get a service that communicates to a Google API.
  
    Args:
      api_name: string The name of the api to connect to.
      api_version: string The api version to connect to.
      scope: A list of strings representing the auth scopes to authorize for the
        connection.
      client_secrets_path: string A path to a valid client secrets file.
  
    Returns:
      service: the service that is connected to the specified API.
    """

    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(
        client_secrets_path, scope=scope,
        message=tools.message_if_missing(client_secrets_path))

    # Prepare credentials, and authorize HTTP object with them.
    # If the credentials don't exist or are invalid run through the native client
    # flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.

    storage = file.Storage(api_name + '.dat')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage)
    http = credentials.authorize(http=httplib2.Http())

    # Build the service object.
    service = build(api_name, api_version, http=http)
    return service

def get_container(service, gtm_account_id, container_id):

    """Returns the selected container for an account.
  
    Args:
      service: the Tag Manager service object.
      gtm_account_id: the Tag Manager account ID from which to retrieve the
        container.
      container_id: the container ID of the container to return.  
  
    Returns:
      container: The container picked.
    """

    container = service.accounts().containers().workspaces().get(path='accounts/%s/containers/%s' % (gtm_account_id, container_id)).execute()
    return container

def get_workspace(service, gtm_account_id, container_id, workspace_id):

    """Gets the selected workspace.

    Args:
      service: the Tag Manager service object.
      gtm_account_id: the Tag Manager account ID from which to retrieve the workspace.
      container_id: the container ID in which the workspace is located.
      workspace_id: the name of the workspace to return.

    Returns:
      workspace: The selected workspace.
    """

    workspace = service.accounts().containers().workspaces().get(path='accounts/%s/containers/%s/workspaces/%s' % (gtm_account_id, container_id, workspace_id)).execute()
    return workspace

def get_tag(service, workspace, tag_id):

    """Gets a tag object.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace to create a tag within.
      tag_id: the ID of the tag object to return.
    Returns:
      the selected tag.
    """

    return service.accounts().containers().workspaces().tags().get(parent=workspace['path'], id=tag_id).execute()

##########################################################################
#                       LIST utils                                       #
##########################################################################

def list_containers(service, gtm_account_id):

    """Lists the workspaces inside a container.

    Args:
      service: the Tag Manager service object.
      gtm_account_id: the Tag Manager account ID from which to retrieve the workspace.
      container: the container object in which the workspace is located.

    Returns:
      workspace_list: a dictionary which 'workspace' key contains a list of workspace objects.
    """

    containers_list = service.accounts().containers().list(parent='accounts/%s' % gtm_account_id).execute()
    return containers_list

def list_workspaces(service, container):

    """Lists the workspaces inside a container.

    Args:
      service: the Tag Manager service object.
      gtm_account_id: the Tag Manager account ID from which to retrieve the workspace.
      container: the container object in which the workspace is located.

    Returns:
      workspace_list: a dictionary which 'workspace' key contains a list of workspace objects.
    """

    workspace_list = service.accounts().containers().workspaces().list(parent=container['path']).execute()
    return workspace_list


def list_tags(service, workspace):

    """Lists the tags inside a workspace.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace object in which we want to list the tags.

    Returns:
      tags_list: a dictionary which 'tag' key contains a list of tag objects.
    """

    tags_list = service.accounts().containers().workspaces().tags().list(parent=workspace['path']).execute()
    return tags_list

def list_triggers(service, workspace):

    """Lists the triggers inside a workspace.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace object in which we want to list the triggers.

    Returns:
      triggers_list: a dictionary which 'trigger' key contains a list of trigger objects.
    """

    triggers_list = service.accounts().containers().workspaces().triggers().list(parent=workspace['path']).execute()
    return triggers_list

def list_variables(service, workspace):

    """Lists the variables inside a workspace.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace object in which we want to list the variables.

    Returns:
      variables_list: a dictionary which 'variable' key contains a list of variable objects.
    """

    variables_list = service.accounts().containers().workspaces().variables().list(parent=workspace['path']).execute()
    return variables_list

def list_folders(service, workspace):

    """Lists the folders inside a workspace.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace object in which we want to list the variables.

    Returns:
      folders_list: a dictionary which 'folder' key contains a list of folder objects.
    """

    folders_list = service.accounts().containers().workspaces().folders().list(parent=workspace['path']).execute()
    return folders_list

##########################################################################
#                       GET by name utils                                #
##########################################################################

def get_workspace_byname(service, container, workspace_name='Default Workspace'):

    """Gets the selected workspace.

    Args:
      service: the Tag Manager service object.
      container: the container object from which we want to select a workspace.
      workspace_name: the name of the workspace to return.

    Returns:
      workspace: The selected workspace object. None if workspace not found.
    """

    workspace = None
    workspaces_list = service.accounts().containers().workspaces().list(parent=container['path']).execute()
    for workspace in workspaces_list['workspace']:
        if workspace['name'] == workspace_name:
            workspace = service.accounts().containers().workspaces().get(path=workspace['path']).execute()
    return workspace

def get_folder_byname(service, workspace, folder_name):

    """Gets the selected folder.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace object from which we want to select a folder.
      folder_name: the name of the folder to return.

    Returns:
      f: The selected folder object. None if folder not found.
    """

    f = None
    if list_folders(service, workspace) != {}:
        for folder in list_folders(service, workspace)['folder']:
            if folder['name'] == folder_name:
                f = folder
    return f

def get_tag_byname(service, workspace, tag_name):

    """Gets the selected tag.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace object from which we want to select a tag.
      tag_name: the name of the tag to return.

    Returns:
      t: The selected tag object. None if tag not found.
    """

    t = None
    for tag in list_tags(service, workspace)['tag']:
        if tag['name'] == tag_name:
            t = tag
    return t

def get_trigger_byname(service, workspace, trigger_name):

    """Gets the selected trigger.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace object from which we want to select a trigger.
      trigger_name: the name of the trigger to return.

    Returns:
      trig: The selected trigger object. None if trigger not found.
    """

    trig = None
    for trigger in list_triggers(service, workspace)['trigger']:
        if trigger['name'] == trigger_name:
            trig = trigger
    return trig

def get_variable_byname(service, workspace, variable_name):

    """Gets the selected variable.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace object from which we want to select a variable.
      variable_name: the name of the variable to return.

    Returns:
      v: The selected variable object. None if variable not found.
    """

    v = None
    for variable in list_variables(service, workspace)['variable']:
        if variable['name'] == variable_name:
            v = variable
    return v

##########################################################################
#                       MOVE utils                                       #
##########################################################################



def move_entity_to_folder(service, workspace, folder_name, tag_name=None, trigger_name=None, variable_name=None):

    """Moves a GTM object to a given folder.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace object from which we want to select a GTM object.
      folder_name: the name of the folder to move a GTM object to.
      tag_name: the name of the tag to move. Has priority on trigger_name and variable_name if these other fields are filled in as well.
      trigger_name: the name of the trigger to move. Has priority on variable_name if this other field is filled in as well.
      variable_name: the name of the variable to move.
    """

    f = get_folder_byname(service, workspace, folder_name)

    i = [tag_name, trigger_name, variable_name]

    if tag_name is not None:
        t = get_tag_byname(service, workspace, tag_name)
        service.accounts().containers().workspaces().folders().move_entities_to_folder(path=f['path'],
                                                                                       tagId=t['tagId'], body=f).execute()
    elif trigger_name is not None:
        tr = get_trigger_byname(service, workspace, trigger_name)
        service.accounts().containers().workspaces().folders().move_entities_to_folder(path=f['path'],
                                                                                       triggerId=tr['triggerId'], body=f).execute()
    elif variable_name is not None:
        v = get_variable_byname(service, workspace, variable_name)
        service.accounts().containers().workspaces().folders().move_entities_to_folder(path=f['path'],
                                                                             variableId=v['variableId'], body=f).execute()

##########################################################################
#                       CREATE utils                                     #
##########################################################################


def create_workspace(service, container, workspace_name):

    """Creates a workspace.

    Args:
      service: the Tag Manager service object.
      container: the container to insert the workspace within.
      workspace_name: name of the created workspace

    Returns:
      The created workspace.
    """

    return service.accounts().containers().workspaces().create(
        parent=container['path'],
        body={
            'name': workspace_name,
        }).execute()

def create_ecommerce_tag(service, workspace, ga_settings_variable, track_type, use_data_layer, ecommerce_macro, tag_name, trigger_name):

    """Create a Universal Analytics Enhanced Ecommerce tag.
  
    Args:
      service: the Tag Manager service object.
      workspace: the workspace to create a tag within.
      ga_settings_variable: the name of the GA Settings variable defined in GTM, e.g. '{{GA_settings}}'
      tag_name: name given to the created tag.
    Returns:
      the created tag.
    """

    created_tag = EzTag()
    created_tag.set_tag_type('ua')
    created_tag.set_enhanced_ecommerce(ga_settings_variable, track_type, use_data_layer, ecommerce_macro)
    created_tag.set_trigger(str(get_trigger_byname(service, workspace, trigger_name)['triggerId']))
    pprint.pprint(created_tag.body)
    os.system("pause")
    return created_tag.upload_tag(service, workspace['path'], tag_name)

def create_trigger(service, workspace, trigger_name, trigger_type, filters=[]):

    """Create a Google Tag Manager trigger object.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace to create a trigger within.
      trigger_name: the name given to the created trigger. It is not possible to overwrite an existing trigger name.
      trigger_type: the type of trigger selected. See valid values in the trigger object description in API doc.
      filters: the list of filters to add to the trigger. Filters' format must be the following:
        {'variable': '{{Page URL}}', 'condition': 'equals', 'value': 'http://demoshop.sz-analytics.com/'}
        
    Returns: the created trigger.
    """

    new_trigger = EzTrigger()
    new_trigger.set_type(trigger_type)

    for fi in filters:
        new_trigger.add_filter(fi['variable'], fi['condition'], fi['value'])

    return new_trigger.upload_trigger(service=service, parent_path=workspace['path'], trigger_name=trigger_name)

def create_folder(service, workspace, folder_name):

    """Create a Google Tag Manager folder object.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace to create a trigger within.
      folder_name: the name of the folder to create.
    """

    try:
        service.accounts().containers().workspaces().folders().create(parent=workspace['path']).execute()
    except HttpError:
        print('HttpError: Could not create folder ' + folder_name + '. Please make sure the name is not already used by an existing folder.')

##########################################################################
#                       DELETE utils                                     #
##########################################################################

def delete_tag(service, workspace, tag_id):

    """Deletes a tag object.

    Args:
      service: the Tag Manager service object.
      gtm_account_id: the Tag Manager account ID.
      container_id: the ID of the container to create a tag within.
      workspace_id: the ID of the workspace to create a tag within.
      tag_id: the ID of the tag object to delete.
    """
    tag_path = str(workspace['path']) + '/tags/' + str(tag_id)
    return service.accounts().containers().workspaces().tags().delete(path=tag_path).execute()

def delete_trigger(service, workspace, trigger_id):

    """Deletes a trigger object.

    Args:
      service: the Tag Manager service object.
      gtm_account_id: the Tag Manager account ID.
      container_id: the ID of the container to create a trigger within.
      workspace_id: the ID of the workspace to create a trigger within.
      trigger_id: the ID of the trigger object to delete.
    """
    trigger_path = str(workspace['path']) + '/triggers/' + str(trigger_id)
    return service.accounts().containers().workspaces().triggers().delete(path=trigger_path).execute()

def delete_variable(service, workspace, variable_id):

    """Deletes a trigger object.

    Args:
      service: the Tag Manager service object.
      gtm_account_id: the Tag Manager account ID.
      container_id: the ID of the container to create a trigger within.
      workspace_id: the ID of the workspace to create a trigger within.
      trigger_id: the ID of the trigger object to delete.
    """
    variable_path = str(workspace['path']) + '/variables/' + str(variable_id)
    return service.accounts().containers().workspaces().variables().delete(path=variable_path).execute()

##########################################################################
#                       AUTOCREATE utils                                 #
##########################################################################

def autocreate_tags(service, workspace, gas_var_name, folder_name, eec_tag_type='pageview', virtual_pageviews_list=[]):

    """Automatically creates Google Tag Manager tag objects from the implementation guide.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace to create a trigger within.
      gas_variable: the name of the Google Analytics settings variable to reference in the tag
      folder_name: the name of the folder to store the tags in.
      eec_tag_type: the type of tag which will transfer Enhanced Ecommerce objects to GA. Defaults to 'pageview'.
      virtual_pageviews_list: list object containing all the event keys which are virtual pageviews and not events
    """

    if eec_tag_type == 'pageview':
        create_ecommerce_tag(service, workspace, gas_var_name, eec_tag_type, use_data_layer='true',
                             ecommerce_macro=None, tag_name='UA EEC Page', trigger_name='allPages')
    # elif eec_tag_type == 'event':
    #     create_ecommerce_tag(service, workspace, gas_var_name, eec_tag_type, use_data_layer=True,
    #                          ecommerce_macro=None, tag_name='UA EEC All Events')
    else:
        print("""TagTypeError: eec_tag_type parameter must be either 'pageview' or 'event'.""")
    #Create the all pageviews tag
    #Create the all_events tag

    pass

def autocreate_triggers(service, workspace, events_dict, folder_name, virtual_pageviews_list=[]):

    """Automatically creates Google Tag Manager trigger objects from the implementation guide.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace to create a trigger within.
      events_dict: events dictionary from the implementation guide parser.
      folder_name: the name of the folder to store the triggers in.
      virtual_pageviews_list: list object containing all the event keys which are virtual pageviews and not events
    """

    event_keys = []

    for d in events_dict['raw']:
        if 'event' in d.keys() and d.get('event') not in virtual_pageviews_list:
            event_keys.append(d['event'])

    ev_regex = events_regex(event_keys)


    events_trig = EzTrigger()
    events_trig.add_regex(regex=ev_regex)
    trig_name = 'allEvents'

    try:
        events_trig.upload_trigger(service, workspace['path'], trig_name)
        move_entity_to_folder(service, workspace, folder_name, trigger_name=events_trig.body['name'])
        print('Successfully created trigger ' + trig_name + ' and moved it to folder ' + folder_name)
    except HttpError:
        print('HttpError: Could not create trigger ' + trig_name + '. Check if the name is not already taken.')
        pass

    trig_name = 'allPages'
    pageview_trig = EzTrigger()
    pageview_trig.set_type('pageview')

    try:
        pageview_trig.upload_trigger(service, workspace['path'], trig_name)
        move_entity_to_folder(service, workspace, folder_name, trigger_name=pageview_trig.body['name'])
        print('Successfully created trigger ' + trig_name + ' and moved it to folder ' + folder_name)
    except HttpError:
        print('HttpError: Could not create trigger ' + trig_name + '. Check if the name is not already taken.')
        pass

def autocreate_dl_variables(service, workspace, events_dict, folder_name, virtual_pageviews_list=[]):

    """Automatically create Google Tag Manager dataLayer variables objects from the implementation guide.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace to create a trigger within.
      events_dict: events dictionary from the implementation guide parser.
      folder_name: the name of the folder to store the triggers in.
      virtual_pageviews_list: list object containing all the event keys which are virtual pageviews and not events
    """

    event_keys = []
    for d in events_dict['raw']:
         event_keys.extend(d.keys())

    for k in set(event_keys):
        try:
            new_dl_var = EzVar()
            new_dl_var.set_dl_var(k)
            new_dl_var.upload_var(service, workspace['path'], 'DL_' + k)
            move_entity_to_folder(service, workspace, folder_name, variable_name=new_dl_var.body['name'])
            print('Successfully created DL variable ' + k + ' and moved it to folder ' + folder_name)
        except HttpError:
            print('HttpError: Could not create variable ' + k + '. Check if the name is not already taken.')

##########################################################################
#                       AUTODELETE utils                                 #
##########################################################################

def autodelete_folder_entities(service, workspace, folder_name):

    """Automatically deletes all Google Tag Manager objects from a given folder.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace to create a trigger within.
      folder_name: the name of the folder to delete objects from.
    """

    f = get_folder_byname(service, workspace, folder_name)
    e = service.accounts().containers().workspaces().folders().entities(path=f['path']).execute()

    stack = []

    try:
        if 'tag' in e:
            for obj in e.get('tag'):
                delete_tag(service, workspace, obj['tagId'])
                print('Successfully deleted tag ' + obj['name'] + ' from folder ' + folder_name)
        if 'trigger' in e:
            for obj in e.get('trigger'):
                delete_trigger(service, workspace, obj['triggerId'])
                print('Successfully deleted trigger ' + obj['name'] + ' from folder ' + folder_name)
        if 'variable' in e:
            for obj in e.get('variable'):
                delete_variable(service, workspace, obj['variableId'])
                print('Successfully deleted variable ' + obj['name'] + ' from folder ' + folder_name)
    except HttpError:
        print('HttpError: Could not delete object ' + obj['name'] + '. It is probably referenced in other objects.')
        stack.append(obj)