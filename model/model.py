from pywr.core import Model, Input, Output, Link, Storage
from pywr.parameters.parameters import ArrayIndexedParameter
from pywr.recorders import NumpyArrayNodeRecorder


# create the model
def create_model(network, template, start, end, ts, params, debug_gain=False, debug_loss=False):

    input_types = ['Inflow Node', 'Catchment']
    output_types = ['Outflow Node', 'Urban Demand', 'General Demand']
    storage_types = ['Reservoir', 'Groundwater']

    model = Model(start=start, end=end, timestep=10, solver='glpk')

    # -----------------GENERATE NETWORK STRUCTURE -----------------------

    # create node dictionaries by name and id
    node_lookup_name = {}
    node_lookup_id = {}
    for node in network['nodes']:
        types = [t for t in node['types'] if t['template_id'] == template['id']]
        node_lookup_name[node.get('name')] = {
            'type': types[0]['name'] if types else None,
            'id': node['id']
        }
        node_lookup_id[node.get("id")] = {
            'type': types[0]['name'] if types else None,
            'name': node.get("name"),
            'attributes': node['attributes']
        }

    # create link lookups and pywr links
    link_lookup = {}
    link_lookup_id = {}
    link_types = ['Conveyance', 'Pipeline', 'Tunnel']
    pywr_links = {}

    for link in network['links']:
        name = link['name']
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
            'name': link['name'],
            'type': link['types'][0]['name'],
            'node_1_id': node_1_id,
            'node_2_id': node_2_id,
            'from_slot': node_lookup_id[node_1_id]['connect_out'] - 1,
            'to_slot': node_lookup_id[node_2_id]['connect_in'] - 1,
            'attributes': link['attributes']
        }
        pywr_links[link_id] = Link(model, name=name)

    #  remove unconnected (rogue) nodes from analysis
    connected_nodes = []
    for link_id, trait in link_lookup_id.items():
        connected_nodes.append(trait['node_1_id'])
        connected_nodes.append(trait['node_2_id'])
    rogue_nodes = []
    for node in node_lookup_id:
        if node not in connected_nodes:
            rogue_nodes.append(node)
    for node in rogue_nodes:
        del node_lookup_id[node]

    # create pywr nodes dictionary with format ["name" = pywr type + 'name']
    # for storage and non storage
    storage = {}

    non_storage = {}

    # TODO: change looping variable notation
    for node_id, node_trait in node_lookup_id.items():
        types = node_trait['type']
        name = node_trait['name']
        if types in storage_types:
            num_outputs = node_trait.get('connect_in', 0)
            num_inputs = node_trait.get('connect_out', 0)
            storage[node_id] = Storage(model, name=name, num_outputs=num_outputs, num_inputs=num_inputs)
        elif types in output_types:
            non_storage[node_id] = Output(model, name=name)
        elif types in input_types:
            non_storage[node_id] = Input(model, name=name)
        else:
            non_storage[node_id] = Link(model, name=name)

    # create network connections
    # must assign connection slots for storage
    # TODO: change looping variable notation
    for link_id, link_trait in link_lookup_id.items():
        up_node = link_trait['node_1_id']
        down_node = link_trait['node_2_id']
        if node_lookup_id[up_node]['type'] not in storage_types and \
                node_lookup_id[down_node]['type'] not in storage_types:
            non_storage[up_node].connect(pywr_links[link_id])
            pywr_links[link_id].connect(non_storage[down_node])
        elif node_lookup_id[up_node]['type'] in storage_types and \
                node_lookup_id[down_node]['type'] not in storage_types:
            storage[up_node].connect(pywr_links[link_id], from_slot=link_trait['from_slot'])
            pywr_links[link_id].connect(non_storage[down_node])
        elif node_lookup_id[up_node]['type'] not in storage_types and \
                node_lookup_id[down_node]['type'] in storage_types:
            non_storage[up_node].connect(pywr_links[link_id])
            pywr_links[link_id].connect(storage[down_node], to_slot=link_trait['to_slot'])
        else:
            storage[up_node].connect(pywr_links[link_id], from_slot=link_trait['from_slot'])
            pywr_links[link_id].connect(storage[down_node], to_slot=link_trait['to_slot'])


    # -------------------- INPUT DATA --------------------

    return model