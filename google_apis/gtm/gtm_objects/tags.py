
class EzTag:

    def __init__(self):

        self.body = {
            'parameter': [],
            'type': '',
            'name': ''
        }

        #Add other tags: Adwords Remarketing Pixel, Facebook Pixel, etc.

    def upload_tag(self, service, parent_path, tag_name):

        self.body['name'] = tag_name
        service.accounts().containers().workspaces().tags().create(parent=parent_path, body=self.body).execute()

    def set_tag_type(self, tag_type='ua'):

        self.body['type'] = tag_type
            
    def set_enhanced_ecommerce(self, ga_settings_variable='{{GA Settings}}', track_type='pageview', use_data_layer='true', ecommerce_macro=None,
                                category='Category', action='Action', label='Label', value=1):

        self.set_tag_type('ua')

        params = [
                {'key': 'nonInteraction', 'type': 'boolean', 'value': 'false'},
                {'key': 'overrideGaSettings', 'true': 'boolean', 'value': 'true'},
                {'key': 'trackType', 'type': 'template', 'value': 'TRACK_' + track_type.upper()},
                {'key': 'gaSettings', 'type': 'template', 'value': ga_settings_variable},
                {'key': 'eventCategory', 'type': 'template', 'value': category},
                {'key': 'eventAction', 'type': 'template', 'value': action},
                {'key': 'eventLabel', 'type': 'template', 'value': label},
                {'key': 'eventValue', 'type': 'template', 'value': value},
                {'key': 'enableEcommerce', 'type': 'boolean', 'value': 'true'},
                {'key': 'useEcommerceDataLayer', 'type': 'boolean', 'value': use_data_layer},
                {'key': 'ecommerceMacroData', 'type': 'template', 'value': ecommerce_macro}
        ]

        if use_data_layer == 'true':
            params = [d for d in params if d.get('key') != 'ecommerceMacroData']

        if track_type == 'pageview':
            params = [d for d in params if d.get('key') not in ['nonInteraction', 'eventCategory', 'eventAction', 'eventLabel', 'eventValue']]

        self.body['parameter'] = params

    def set_trigger(self, triggerId):

        self.body['firingTriggerId'] = [triggerId]
        self.body['tagFiringOption'] = 'oncePerEvent'
