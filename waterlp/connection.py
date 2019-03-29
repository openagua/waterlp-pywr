import json

from requests import post


class connection(object):

    def __init__(self, args=None, scenario_ids=None, log=None):
        self.url = args.data_url
        self.filename = args.filename
        self.app_name = args.app_name
        self.session_id = args.session_id
        self.user_id = int(args.user_id)
        self.log = log

        self.network_id = int(args.network_id)
        self.template_id = int(args.template_id) if args.template_id else None

        get_network_params = dict(
            network_id=self.network_id,
            include_data='Y',
            scenario_ids=scenario_ids,
            summary='N'
        )
        if self.template_id:
            get_network_params.update({'template_id': self.template_id})

        if args.filename:
            with open(args.filename) as f:
                data = json.load(f, object_hook=JSONObject)
                self.network = data.get('network')
                self.template = data.get('template')
                self.template_attributes = data.get('template_attributes')
                self.template_id = self.template.get('id')

        else:
            response = self.call('get_network', get_network_params)
            if 'faultcode' in response:
                if 'session' in response.get('faultcode', '').lower():
                    self.login(username=args.hydra_username, password=args.hydra_password)
                response = self.call('get_network', get_network_params)

            self.network = response
            self.template_id = self.template_id or self.network.layout.get('active_template_id')
            self.template = self.template_id and self.call('get_template', {'template_id': self.template_id})

        # create some useful dictionaries
        # Since pyomo doesn't know about attribute ids, etc., we need to be able to relate
        # pyomo variable names to resource attributes to be able to save data back to the database.
        # the res_tattrs dictionary lets us do that by relating pyomo indices and variable names to
        # the resource attribute id.

        # dictionary to store resource attribute dataset types
        self.attr_meta = {}

        # dictionary for looking up attribute ids

        # dictionary to store resource attribute ids
        self.resource_attributes = {}
        self.res_attr_lookup = {}
        self.attr_ids = {}
        self.raid_to_res_name = {}
        self.attr_id_lookup = {}
        self.node_names = {}
        self.tattrs = {}
        self.types = {}
        ttypes = {tt.id: tt for tt in self.template.types}

        def process_resource(resource_type, resource):
            rtypes = [rt for rt in resource.types if rt.template_id == self.template_id]
            if rtypes:
                rtype = rtypes[0]
            else:
                return
            ttype = ttypes[rtype.id]
            self.types[(resource_type, resource.id)] = {'id': ttype.id, 'name': ttype.name}
            tattrs = {ta.attr_id: ta for ta in ttype.typeattrs}
            for ra in resource.attributes:
                if ra.attr_id in tattrs:
                    key = (resource_type, resource.id, ra.attr_id)
                    self.tattrs[key] = tattrs[ra.attr_id]
                    # TODO: confirm the following doesn't overwrite attributes with a different dimension
                    self.attr_id_lookup[(resource_type, resource.id, ra.attr_name.lower())] = ra.attr_id
                    self.res_attr_lookup[key] = ra.id
                    self.attr_ids[ra.id] = ra.attr_id
                    self.raid_to_res_name[ra.id] = resource.name

        process_resource('network', self.network)
        for node in self.network.nodes:
            process_resource('node', node)
        for link in self.network.links:
            process_resource('link', link)

    def call(self, func, args):

        data = json.dumps({func: args})

        headers = {'Content-Type': 'application/json', 'appname': self.app_name}
        cookie = {'beaker.session.id': self.session_id if func != 'login' else None, 'appname:': self.app_name}
        response = post(self.url, data=data, headers=headers, cookies=cookie, timeout=500)

        if not response.ok:
            try:
                content = json.loads(response.content.decode(), object_hook=JSONObject)
                fc, fs = content['faultcode'], content['faultstring']
                # print(content)
            except:
                print('Something went wrong. Check command sent.')
                print("URL: %s" % self.url)
                print("Call: %s" % data)

                if response.content != '':
                    print(response.content)
                else:
                    print("Something went wrong. An unknown server has occurred.")
        else:
            content = json.loads(response.content.decode(), object_hook=JSONObject)
            if func == 'login':
                self.session_id = response.cookies['beaker.session.id']

        return content

    def get_basic_network(self):
        if self.filename:
            return self.network
        else:
            return self.call('get_network',
                             {'network_id': self.network.id, 'include_data': 'N', 'summary': 'N',
                              'include_resources': 'N'})

    # def get_template_attributes(self):
    #     if self.filename:
    #         return self.template_attributes
    #     else:
    #         return self.call('get_template_attributes', {'template_id': self.template.id})
    #

    def get_res_attr_data(self, **kwargs):
        res_attr_data = self.call(
            'get_resource_attribute_data',
            dict(
                ref_key=kwargs['resource_type'].upper(),
                ref_id=kwargs['resource_id'],
                scenario_id=kwargs['scenario_id'],
                attr_id=kwargs['attr_id'] if 'attr_id' in kwargs else None
            )
        )
        return res_attr_data

    def login(self, username=None, password=None):
        if username is None:
            err = 'Error. Username not provided.'
        self.call('login', {'username': username, 'password': password})
        return

    def dump_results(self, resource_scenario):
        return self.call('update_scenario', {'scen': resource_scenario, 'return_summary': 'Y'})


class JSONObject(dict):
    def __init__(self, obj_dict):
        for k, v in obj_dict.items():
            self[k] = v
            setattr(self, k, v)
