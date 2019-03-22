import datetime
import pandas
from pywr.core import Model, Input, Output, Link, Timestepper
from pywr.domains.river import River, Storage, RiverGauge, Catchment
from pywr.parameters import ArrayIndexedParameter, DataFrameParameter, ConstantParameter

from .domains import Hydropower, InstreamFlowRequirement

# from pywr.recorders import (NumpyArrayNodeRecorder, NumpyArrayStorageRecorder)

storage_types = {
    'Reservoir': Storage,
    'Groundwater': Storage,
}
output_types = {
    'Outflow Node': Output,
    'Urban Demand': Output,
    'General Demand': Output,
    'Agricultural Demand': Output,
}
input_types = {
    'Inflow Node': Catchment,
    'Misc Source': Input,
    'Catchment': Catchment,
}
node_types = {
    'Hydropower': Hydropower,
    'Flow Requirement': InstreamFlowRequirement,
}

link_types = {
    'River': River,
}


def negative(value):
    return -abs(value) if type(value) in [int, float] else value


# create the model
class PywrModel(object):
    def __init__(self, network, template, start=None, end=None, step=None,
                 constants=None, variables=None,
                 check_graph=False):

        self.model = None
        self.storage = {}
        self.non_storage = {}
        self.updated = {}  # dictionary for debugging whether or not a param has been updated

        self.create_model(network, template, constants=constants, variables=variables)

        # check network graph
        if check_graph:
            try:
                self.model.check_graph()
            except Exception as err:
                raise Exception('Pywr error: {}'.format(err))

        self.setup(start=start, end=end, step=step)

    def create_model(self, network, template, constants=None, variables=None):

        model = Model(solver='glpk-edge')

        # -----------------GENERATE NETWORK STRUCTURE -----------------------
        # ...and add initial parameter values

        output_ids = []
        input_ids = []

        non_storage_types = list(output_types.keys()) + list(input_types.keys()) + list(node_types.keys())

        def add_value_to_node(res_attr_idx, type_name, attr_name):
            value = constants.pop(res_attr_idx, None)
            if value:
                type_name = type_name.lower()
                attr_name = attr_name.lower()
                (resource_type, resource_id, attr_id) = res_attr_idx
                self.update_param(resource_type, resource_id, type_name, attr_name, value=value)

        # create node dictionaries by name and id
        node_lookup = {}
        for node in network['nodes']:
            name = '{} (node)'.format(node['name'])
            types = [t for t in node['types'] if t['template_id'] == template['id']]
            if not types:
                continue
            if len(types) > 1:
                msg = "Type is ambiguous for {}. Please remove extra types.".format(name)
                raise Exception(msg)
            type_name = types[-1]['name']
            node_lookup[node.get("id")] = {
                'type': type_name,
                'name': name,
                'connect_in': 0,
                'connect_out': 0,
                'attributes': node['attributes']
            }
            if type_name in output_types:
                output_ids.append(node['id'])
            elif type_name in input_types:
                input_ids.append(node['id'])

        # create link lookups and pywr links
        link_lookup = {}
        for link in network['links']:
            residx = ('link', link['id'])
            name = '{} (link)'.format(link['name'])
            types = [t for t in link['types'] if t['template_id'] == template['id']]
            if not types:
                continue
            type_name = types[-1]['name']
            link_id = link['id']
            node_1_id = link['node_1_id']
            node_2_id = link['node_2_id']
            node_lookup[node_1_id]['connect_out'] += 1
            node_lookup[node_2_id]['connect_in'] += 1
            link_lookup[link_id] = {
                'name': name,
                'type': type_name,
                'node_1_id': node_1_id,
                'node_2_id': node_2_id,
                'from_slot': node_lookup[node_1_id]['connect_out'] - 1,
                'to_slot': node_lookup[node_2_id]['connect_in'] - 1,
            }

            if node_1_id in output_ids:
                node = node_lookup[node_1_id]
                msg = 'Topology error: Output {} appears to be upstream of {}'.format(node['name'], name)
                raise Exception(msg)
            elif node_2_id in input_ids:
                node = node_lookup[node_2_id]
                msg = 'Topology error: Input {} appears to be downstream of {}'.format(node['name'], name)
                raise Exception(msg)

            LinkType = link_types.get(type_name, Link)

            self.non_storage[residx] = LinkType(model, name=name)

            for ra in link['attributes']:
                res_attr_idx = ('link', link['id'], ra['attr_id'])
                add_value_to_node(res_attr_idx, type_name, ra['attr_name'])

        # Q/C

        # remove unconnected links
        d = []
        for link_id, link in link_lookup.items():
            if link['node_1_id'] not in node_lookup or link['node_2_id'] not in node_lookup:
                d.append(link_id)
        for link_id in d:
            del link_lookup[link_id]

        connected_nodes = []
        for link_id, link in link_lookup.items():
            connected_nodes.append(link['node_1_id'])
            connected_nodes.append(link['node_2_id'])

        # remove unconnected nodes
        d = []
        for node_id in node_lookup:
            if node_id not in connected_nodes:
                d.append(node_id)
        for node_id in d:
            del node_lookup[node_id]

        # create pywr nodes dictionary with format ["name" = pywr type + 'name']
        # for storage and non storage

        for node_id, node in node_lookup.items():
            residx = ('node', node_id)
            type_name = node['type']
            name = node['name']
            connect_in = node.get('connect_in', 0)
            connect_out = node.get('connect_out', 0)
            if (type_name in storage_types or connect_out > 1) and type_name not in non_storage_types:
                # initial_volume = initial_volumes.get(node_id, 0.0) if initial_volumes is not None else 0.0
                self.storage[node_id] = Storage(
                    model,
                    name=name,
                    num_outputs=connect_in,
                    num_inputs=connect_out,
                    # initial_volume=initial_volume
                )
                if type_name not in storage_types:
                    self.storage[node_id].max_volume = 0.0
            else:

                if type_name in input_types:
                    NodeType = input_types[type_name]
                elif type_name in output_types:
                    NodeType = output_types[type_name]
                elif type_name in node_types:
                    NodeType = node_types[type_name]
                elif connect_in > 1:
                    NodeType = River
                else:
                    NodeType = Link

                self.non_storage[residx] = NodeType(model, name=name)

            for ra in node['attributes']:
                res_attr_idx = ('node', node_id, ra['attr_id'])
                try:
                    add_value_to_node(res_attr_idx, type_name, ra['attr_name'])
                except Exception as err:
                    print(err)
                    raise

        # create network connections
        # must assign connection slots for storage
        # TODO: change looping variable notation
        for link_id, link in link_lookup.items():
            node_1_id = link['node_1_id']
            node_2_id = link['node_2_id']

            _link = self.non_storage[('link', link_id)]
            up_storage = self.storage.get(node_1_id)
            up_node = self.non_storage.get(('node', node_1_id))
            down_storage = self.storage.get(node_2_id)
            down_node = self.non_storage.get(('node', node_2_id))

            if up_storage:
                up_storage.connect(_link, from_slot=link['from_slot'])
            else:
                up_node.connect(_link)

            if down_storage:
                _link.connect(down_storage, to_slot=link['to_slot'])
            else:
                _link.connect(down_node)

        self.model = model

    def setup(self, start, end, step):

        self.update_timesteps(
            start=start,
            end=end,
            step=step
        )

        try:
            self.model.setup()
            return
        except Exception as err:
            print(err)
            raise

    def update_timesteps(self, start, end, step):
        self.model.timestepper = Timestepper(
            pandas.to_datetime(start),  # start
            pandas.to_datetime(end),  # end
            datetime.timedelta(step)  # step
        )

    def update_param(self, resource_type, resource_id, type_name, attr_name, value):

        res_idx = (resource_type, resource_id)
        attr_idx = (resource_type, resource_id, attr_name)

        # if attr_idx in self.updated:
        #     return

        # self.updated[attr_idx] = True

        ta = (type_name.lower(), attr_name.lower())

        if ta == ('catchment', 'runoff'):
            self.non_storage[res_idx].flow = value
        elif ta == ('reservoir', 'initial storage'):
            self.storage[resource_id].initial_volume = value
        elif 'demand' in type_name:
            if attr_name == 'value':
                self.non_storage[res_idx].cost = negative(value)
            elif attr_name == 'demand':
                self.non_storage[res_idx].max_flow = value
        elif type_name == 'flow requirement':
            if attr_name == 'requirement':
                self.non_storage[res_idx].mrf = value
            elif attr_name == 'violation cost':
                self.non_storage[res_idx].mrf_cost = negative(value)
        if type_name == 'hydropower':
            if attr_name == 'water demand':
                self.non_storage[res_idx].base_flow = value
            elif attr_name == 'base value':
                self.non_storage[res_idx].base_cost = negative(value)
            elif attr_name == 'turbine capacity':
                self.non_storage[res_idx].turbine_capacity = value
            elif attr_name == 'excess value':
                self.non_storage[res_idx].excess_cost = negative(value)
        elif attr_name == 'storage demand':
            self.storage[resource_id].max_volume = value
        elif attr_name == 'storage value':
            self.storage[resource_id].cost = negative(value)
        elif attr_name == 'storage capacity':
            self.storage[resource_id].max_volume = value
        elif attr_name == 'inactive pool':
            self.storage[resource_id].min_volume = value
        elif attr_name == 'flow capacity':
            self.non_storage[res_idx].max_flow = value

        return

    # def init_params(self, params, variables, block_params):
    #
    #     for param_name, param in params.items():
    #
    #         data_type = param['data_type']
    #         resource_type = param['resource_type']
    #         attr_name = param['attr_name']
    #         unit = param['unit']
    #         intermediary = param['intermediary']
    #
    #         if intermediary or resource_type == 'network':
    #             continue
    #
    #         param_definition = None
    #
    #         initial_values = variables.get(param_name, None)
    #
    #         if param['is_var'] == 'N':
    #
    #             mutable = True  # assume all variables are mutable
    #             default = 0  # TODO: define in template rather than here
    #
    #             if data_type == 'scalar':
    #                 _param = ConstantParameter(self.model, initial_values)
    #
    #             elif data_type == 'timeseries':
    #                 if initial_values:
    #                     if attr_name in block_params:
    #                         _param = DataframeParameter(self.model, initial_values)
    #                     else:
    #                         _param = ArrayIndexedParameter(self.model, initial_values)
    #
    #             elif data_type == 'array':
    #                 continue  # placeholder
    #
    #             else:
    #                 # this includes descriptors, which have no place in the LP model yet
    #                 continue
    #     return
