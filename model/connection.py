import json

from attrdict import AttrDict
from requests import post


class connection(object):

    def __init__(self, args=None, scenario_ids=None, log=None):
        self.url = args.data_url
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
        # the res_attrs dictionary lets us do that by relating pyomo indices and variable names to
        # the resource attribute id.

        # dictionary to store resource attribute dataset types
        self.attr_meta = {}

        # dictionary for looking up attribute ids

        self.attrs = AttrDict()
        for tt in self.template.types:
            res_type = tt.resource_type.lower()
            if res_type not in self.attrs.keys():
                self.attrs[res_type] = AttrDict()
            for ta in tt.typeattrs:
                self.attrs[res_type][ta.attr_id] = AttrDict({
                    'name': ta.attr_name,
                    'dtype': ta.data_type,
                    'unit': ta.unit,
                    'dim': ta.dimension
                })

        # dictionary to store resource attribute ids
        self.resource_attributes = {}
        self.res_attr_lookup = {'node': {}, 'link': {}}
        self.attr_ids = {}
        self.raid_to_res_name = {}
        self.node_names = {}

        for n in self.network.nodes:
            self.node_names[n.id] = n.name
            for ra in n.attributes:
                if ra.attr_id in self.attrs.node:
                    attr_name = self.attrs.node[ra.attr_id]['name']
                    self.res_attr_lookup['node'][(n.id, attr_name)] = ra.id
                    self.attr_ids[ra.id] = ra.attr_id
                    self.raid_to_res_name[ra.id] = n.name

        for l in self.network.links:
            for ra in l.attributes:
                if ra.attr_id in self.attrs.link:
                    self.res_attr_lookup['link'][
                        (l.node_1_id, l.node_2_id, self.attrs.link[ra.attr_id]['name'])] = ra.id
                    self.attr_ids[ra.id] = ra.attr_id
                    self.raid_to_res_name[ra.id] = l.name

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

    def get_res_attr_data(self, **kwargs):
        res_attr_data = self.call(
            'get_resource_attribute_data',
            dict(
                ref_key=kwargs['ref_key'].upper(),
                ref_id=kwargs['ref_id'],
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


class JSONObject(dict):
    def __init__(self, obj_dict):
        for k, v in obj_dict.items():
            self[k] = v
            setattr(self, k, v)
