import datetime
import pandas
from attrdict import AttrDict
from pywr.core import Model, Input, Output, Link, River, Storage, RiverGauge, Catchment, Timestepper

# from pywr.parameters import (ArrayIndexedParameter, DataFrameParameter, ConstantParameter)
# from pywr.recorders import (NumpyArrayNodeRecorder, NumpyArrayStorageRecorder)

# class FlowRequirement(RiverGauge):
#     def __init__(self, *args, **kwargs):
#         self.allow_isolated = False
#
#         super(FlowRequirement, self).__init__(*args, **kwargs)
#
#     @classmethod
#     def load(cls, data, model):
#         del (data["type"])
#         return cls(model, **data)


storage_types = ['Reservoir', 'Groundwater']
output_types = ['Outflow Node', 'Urban Demand', 'General Demand', 'Agricultural Demand']

node_types = {
    'Inflow Node': Catchment,
    'Misc Source': Input,
    'Catchment': Catchment,
    'Hydropower': RiverGauge,
    'Flow Requirement': RiverGauge,
}

link_types = {
    'River': River,
}


# create the model
class PywrModel(object):
    def __init__(self, network, template, start=None, end=None, step=None, initial_volumes=None, check_graph=False):

        self.model = None
        self.storage = {}
        self.non_storage = {}

        self.create_model(network, template, initial_volumes=initial_volumes)

        # check network graph
        if check_graph:
            try:
                self.model.check_graph()
            except Exception as err:
                raise Exception('Pywr error: {}'.format(err))

        self.setup(start=start, end=end, step=step)

    def create_model(self, network, template, initial_volumes=None):

        model = Model(solver='glpk-edge')

        # -----------------GENERATE NETWORK STRUCTURE -----------------------

        output_ids = []

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
            }
            if type_name in output_types:
                output_ids.append(node['id'])

        # create link lookups and pywr links
        link_lookup = {}
        for link in network['links']:
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

            self.non_storage[('link', link_id)] = link_types.get(type_name, Link)(model, name=name)

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

        # TODO: change looping variable notation
        for node_id, node in node_lookup.items():
            type_name = node['type']
            name = node['name']
            if type_name in storage_types:
                num_outputs = node.get('connect_in', 0)
                num_inputs = node.get('connect_out', 0)
                initial_volume = initial_volumes.get(node_id, 0.0) if initial_volumes is not None else 0.0
                self.storage[node_id] = Storage(model, name=name, num_outputs=num_outputs, num_inputs=num_inputs,
                                                initial_volume=initial_volume)
            else:
                if type_name in output_types:
                    node_type = Output(model, name=name)
                else:
                    node_type = node_types.get(type_name, Link)(model, name=name)
                self.non_storage[('node', node_id)] = node_type

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

    def update_param(self, resource_type, resource_id, param_name, value):

        idx = (resource_type, resource_id)

        if param_name == 'nodeRunoff':
            self.non_storage[idx].flow = value
        elif param_name == 'nodeDemand':
            self.non_storage[idx].max_flow = value
        elif param_name in ['nodeWaterDemand', 'nodeRequirement']:
            self.non_storage[idx].mrf = value  # this is a flow requirement
        elif param_name == 'nodeValue':
            self.non_storage[idx].cost = -value
        elif param_name == 'nodeExcessValue':
            self.non_storage[idx].cost = -value
        elif param_name in ['nodeViolationCost', 'nodeBaseValue']:  # this is a flow requirement
            self.non_storage[idx].mrf_cost = -value
        elif param_name == 'nodeTurbineCapacity':
            if idx in self.non_storage:
                self.non_storage[idx].max_flow = value
            elif idx in self.storage: # TODO: add hydropower to reservoirs?
                # self.storage[idx].max_flow = value
                pass
        elif param_name == 'nodeStorageDemand':
            self.storage[resource_id].max_volume = value
        elif param_name == 'nodeStorageValue':
            self.storage[resource_id].cost = -value
        elif param_name == 'nodeStorageCapacity':
            self.storage[resource_id].max_volume = value
        elif param_name == 'nodeInactivePool':
            self.storage[resource_id].min_volume = value
        elif param_name == 'linkFlowCapacity':
            self.non_storage[idx].max_flow = value

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
