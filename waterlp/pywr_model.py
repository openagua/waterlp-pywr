import datetime
import pandas
from pywr.core import Model, Input, Output, Link, Storage, RiverGauge, Timestepper
from pywr.parameters import (ArrayIndexedParameter, DataFrameParameter, ConstantParameter)
from pywr.recorders import (NumpyArrayNodeRecorder, NumpyArrayStorageRecorder)


# create the model
class NetworkModel(object):
    def __init__(self, network, template, solver='glpk', check_graph=False):

        self.storage = {}
        self.non_storage = {}
        self.links = {}

        self.create_model(network, template, solver)

        # check network graph
        if check_graph:
            try:
                self.model.check_graph()
            except Exception as err:
                raise Exception('Pywr error: {}'.format(err))

    def create_model(self, network, template, solver):
        input_types = ['Inflow Node', 'Catchment', 'Misc Source']
        output_types = ['Outflow Node', 'Urban Demand', 'General Demand', 'Agricultural Demand']
        ifr_types = ['Flow Requirement']
        storage_types = ['Reservoir', 'Groundwater']

        model = Model(solver=solver)

        # -----------------GENERATE NETWORK STRUCTURE -----------------------

        # create node dictionaries by name and id
        node_lookup_name = {}
        node_lookup_id = {}
        for node in network['nodes']:
            name = '{} (node)'.format(node['name'])
            types = [t for t in node['types'] if t['template_id'] == template['id']]
            node_lookup_name[name] = {
                'type': types[0]['name'] if types else None,
                'id': node['id']
            }
            node_lookup_id[node.get("id")] = {
                'type': types[0]['name'] if types else None,
                'name': name,
                'attributes': node['attributes']
            }

        # create link lookups and pywr links
        link_lookup = {}
        link_lookup_id = {}
        # link_types = ['Conveyance', 'Pipeline', 'Tunnel']

        for link in network['links']:
            name = '{} (link)'.format(link['name'])
            link_id = link['id']
            node_1_id = link['node_1_id']
            node_2_id = link['node_2_id']
            node_lookup_id[node_2_id]['connect_in'] = node_lookup_id[node_2_id].get('connect_in', 0) + 1
            node_lookup_id[node_1_id]['connect_out'] = node_lookup_id[node_1_id].get('connect_out', 0) + 1
            link_lookup[name] = {
                'id': link_id,
                'node_1_id': node_1_id,
                'node_2_id': node_2_id,
                'from_slot': node_lookup_id[node_1_id]['connect_out'] - 1,
                'to_slot': node_lookup_id[node_2_id]['connect_in'] - 1
            }
            link_lookup_id[link_id] = {
                'name': name,
                'type': link['types'][0]['name'],
                'node_1_id': node_1_id,
                'node_2_id': node_2_id,
                'from_slot': node_lookup_id[node_1_id]['connect_out'] - 1,
                'to_slot': node_lookup_id[node_2_id]['connect_in'] - 1,
                'attributes': link['attributes']
            }
            self.links[link_id] = Link(model, name=name)

        # remove unconnected (rogue) nodes from analysis
        connected_nodes = []
        for link_id, trait in link_lookup_id.items():
            connected_nodes.append(trait['node_1_id'])
            connected_nodes.append(trait['node_2_id'])
        # rogue_nodes = []
        for node in node_lookup_id:
            if node not in connected_nodes:
                node_lookup_id.pop(node, None)
                # rogue_nodes.append(node)
        # for node in rogue_nodes:
        #     del node_lookup_id[node]

        # create pywr nodes dictionary with format ["name" = pywr type + 'name']
        # for storage and non storage

        # TODO: change looping variable notation
        for node_id, node in node_lookup_id.items():
            node_type = node['type']
            name = node['name']
            if node_type in storage_types:
                num_outputs = node.get('connect_in', 0)
                num_inputs = node.get('connect_out', 0)
                self.storage[node_id] = Storage(model, name=name, num_outputs=num_outputs, num_inputs=num_inputs)
            elif node_type in output_types:
                self.non_storage[node_id] = Output(model, name=name)
            elif node_type in input_types:
                self.non_storage[node_id] = Input(model, name=name)
            elif node_type in ifr_types:
                self.non_storage[node_id] = RiverGauge(model, name=name)
            else:
                self.non_storage[node_id] = Link(model, name=name)

        # create network connections
        # must assign connection slots for storage
        # TODO: change looping variable notation
        for link_id, link_trait in link_lookup_id.items():
            up_node = link_trait['node_1_id']
            down_node = link_trait['node_2_id']

            # connect non-storage nodes to non-storage nodes
            if node_lookup_id[up_node]['type'] not in storage_types and \
                    node_lookup_id[down_node]['type'] not in storage_types:
                self.non_storage[up_node].connect(self.links[link_id])
                self.links[link_id].connect(self.non_storage[down_node])

            # connect storage nodes to non-storage nodes
            elif node_lookup_id[up_node]['type'] in storage_types and \
                    node_lookup_id[down_node]['type'] not in storage_types:
                self.storage[up_node].connect(self.links[link_id], from_slot=link_trait['from_slot'])
                self.links[link_id].connect(self.non_storage[down_node])

            # connect non-storage nodes to storage nodes
            elif node_lookup_id[up_node]['type'] not in storage_types and \
                    node_lookup_id[down_node]['type'] in storage_types:
                self.non_storage[up_node].connect(self.links[link_id])
                self.links[link_id].connect(self.storage[down_node], to_slot=link_trait['to_slot'])

            # connect storage nodes to storage nodes
            else:
                self.storage[up_node].connect(self.links[link_id], from_slot=link_trait['from_slot'])
                self.links[link_id].connect(self.storage[down_node], to_slot=link_trait['to_slot'])

        self.model = model

    def update_timesteps(self, start, end, step):
        self.model.timestepper = Timestepper(
            pandas.to_datetime(start),  # start
            pandas.to_datetime(end),  # end
            datetime.timedelta(step)  # step
        )

    def run(self):
        self.model.run()

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
