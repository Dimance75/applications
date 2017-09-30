
class EzTrigger:

    def __init__(self):

        self.body = {
                'name': '',
                'type': '',
                'filter': [
                ]
            }

    def upload_trigger(self, service, parent_path, trigger_name):

        self.body['name'] = trigger_name
        service.accounts().containers().workspaces().triggers().create(parent=parent_path, body=self.body).execute()

    def set_type(self, trigger_type):

        self.body['type'] = trigger_type

    def add_filter(self, variable, condition, value):

        new_filter = {
                        'type': condition,
                        'parameter': [
                            {
                                'type': 'template',
                                'key': 'arg0',
                                'value': variable
                            },
                            {
                                'type': 'template',
                                'key': 'arg1',
                                'value': value
                            }
                        ]
                    }

        self.body['filter'].append(new_filter)

    def add_regex(self, val='{{_event}}', regex='*.?'):
        
        self.body['type'] = 'customEvent'
        self.body.pop('filter', None)
        self.body['customEventFilter'] = [{'parameter': [{'key': 'arg0',
                                         'type': 'template',
                                         'value': val},
                                        {'key': 'arg1',
                                         'type': 'template',
                                         'value': regex}],
                         'type': 'matchRegex'}]
