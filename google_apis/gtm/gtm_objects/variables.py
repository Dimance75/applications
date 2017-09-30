import yaml
import pprint
import copy

class EzVar:

    def __init__(self):

        self.body = {
                        "name": '',
                        "type": '',
                        "parameter": []
                    }

        self.cd_map_structure = {
            'map':
            [
                {'type': 'template', 'value': '', 'key': 'index'},
                {'type': 'template', 'value': '', 'key': 'dimension'}
            ],
            'type': 'map'
        }

        #with open('gtm_objects/eec_events_map.yaml', 'r') as stream:
        self.eec_events_map = None

    def set_dl_var(self, dl_variable_name):
        self.body['type'] = 'v'
        self.body['parameter'] = [
            {'type': 'integer', 'value': '2', 'key': 'dataLayerVersion'},
            {'type': 'boolean', 'value': 'false', 'key': 'setDefaultValue'},
            {'type': 'template', 'value': dl_variable_name, 'key': 'name'}
        ]

    def set_custom_js_var(self, script):
        self.body['type'] = 'jsm'
        self.body['parameter'] = [{'type': 'template', 'value': script, 'key': 'javascript'}]

    def set_ga_settings_var(self, ga_tracking_id):
        self.body['type'] = 'gas'
        self.body['parameter'] = [
            {'type': 'template', 'value': 'auto', 'key': 'cookieDomain'},
            {'type': 'boolean', 'value': 'false', 'key': 'doubleClick'},
            {'type': 'boolean', 'value': 'false', 'key': 'setTrackerName'},
            {'type': 'boolean', 'value': 'false', 'key': 'useDebugVersion'},
            {'type': 'boolean', 'value': 'false', 'key': 'useHashAutoLink'},
            {'type': 'boolean', 'value': 'false', 'key': 'decorateFormsAutoLink'},
            {'type': 'boolean', 'value': 'false', 'key': 'enableLinkId'},
            {'type': 'boolean', 'value': 'false', 'key': 'enableEcommerce'},
            {'type': 'template', 'value': ga_tracking_id, 'key': 'trackingId'}
        ]

    def set_custom_dimensions(self, custom_vars):

        map_list = []
        for k, v in custom_vars.iteritems():
            map_model = self.cd_map_structure
            map_model['map'][0]['value'] = k
            map_model['map'][1]['value'] = v
            map_list.append(dict(map_model))

        self.body['parameter'].append({'list': map_list,
                                       'type': 'list',
                                       'key': 'dimension'})

    def set_eec_lookup_tabs(self, service, parent_path, input_variable='{{Event}}'):

        events_map = self.eec_events_map['ENHANCED_ECOMMERCE_EVENTVARS']
        events_attributes = ['eventCategory', 'eventAction', 'eventLabel', 'eventValue']

        for attribute in events_attributes:

            new_lu_var = copy.deepcopy(self)
            lu_name = 'LU_' + attribute
            new_lu_var.body['type'] = 'smm'

            lu_list = [{'map': [
                {'key': 'key',
                'type': 'template',
                'value': k},
                {'key': 'value',
                'type': 'template',
                'value': d[attribute]}],
                'type': 'map'}
                for k, d in events_map.items()
            ]

            new_lu_var.body['parameter'] = [{'key': 'setDefaultValue',
                            'type': 'boolean',
                            'value': 'false'},

                           {'key': 'input',
                            'type': 'template',
                            'value': input_variable},

                           {'key': 'map',
                            'list': lu_list
                            }]

            new_lu_var.upload_var(service, parent_path, lu_name)

    def upload_var(self, service, parent_path, variable_name):

        self.body['name'] = variable_name
        service.accounts().containers().workspaces().variables().create(parent=parent_path, body=self.body).execute()