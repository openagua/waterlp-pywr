from math import floor
from itertools import product

operators = {
    'multiply': '*',
    'divide': '/',
    'add': '+',
    'subtract': '-',
}


def xrange(start, end, step):
    try:
        step = abs(step) * (end - start) / abs(end - start)
        l = floor(abs((end - start) / (step or 1) + 1))
        return [start + step * i for i in range(l)]

    except:
        return [start]


def make_levels(variation):
    levels = []

    if variation and variation.get('params', {}).get('nvars'):

        concurrency = variation.get('concurrency')
        method = variation.get('method')
        params = variation.get('params')
        nvars = params.get('nvars')
        start = params.get('start')
        end = params.get('end')

        if concurrency in ['independent', 'crosswise']:
            if method == 'nvars':
                step = (end - start) / (nvars - 1)
                levels = xrange(start, end, step)
            elif method == 'step':
                step = params.get('step')
                levels = xrange(start, end, step)
            elif method == 'manual':
                levels = params.get('values')
        elif concurrency == 'concurrent':
            if method == 'range':
                levels = xrange(start, end, (end - start) / (nvars - 1))
            elif method == 'manual':
                levels = params.get('values')

        if nvars and len(levels) == 1 and len(nvars) > 1:
            levels = levels[0] * nvars

    return levels


def get_ref_key(resource):
    return 'x' in resource and 'node' or 'node_1_id' in resource and 'link' or 'nodes' in resource and 'network'


def create_subscenarios(network, template, scenario, scenario_type):
    variations = scenario.layout.get('variations', [])

    subscenarios = []

    if not variations:
        subscenarios = [{'parent_id': scenario.id, 'variations': {}}]
    else:
        concurrency = variations[0].get('concurrency')
        if concurrency == 'independent':
            subscenarios = create_independent_subscenarios(network, template, scenario, scenario_type)
        elif concurrency == 'crosswise':
            subscenarios = create_crosswise_subscenarios(network, template, scenario, scenario_type)
        elif concurrency == 'concurrent':
            subscenarios = create_concurrent_subscenarios(network, template, scenario, scenario_type)

    return subscenarios


def create_independent_subscenarios(network, template, scenario, scenario_type):
    subscenarios = []
    # path = '{ref_key}/{ref_id}/{attr_id}/{val:.3f}'

    for variation in scenario.layout.get('variations', []):

        attr_id = variation.get('attr_id')
        resources = get_resources_with_variation(network, template, scenario, variation)
        values = make_levels(variation)

        for resource in resources:
            ref_key = get_ref_key(resource)
            for value in values:
                subscenarios.append({
                    'parent_id': scenario.id,
                    'type': scenario_type,
                    'variations': {
                        (ref_key, resource.id, attr_id): {
                            'value': value,
                            'operator': variation.get('operator')
                        },
                        # 'path': path.format(ref_key=ref_key, ref_id=resource.id, attr_id=attr_id, val=value)
                    }
                })

    return subscenarios


def create_crosswise_subscenarios(network, template, scenario, scenario_type):
    subscenarios = []

    variations = scenario.layout.get('variations', [])

    values_lookup = {}
    resources_lookup = {}
    for variation in variations:
        values_lookup[variation['id']] = make_levels(variation)
        resources_lookup[variation['id']] = get_resources_with_variation(network, template, scenario, variation)

    all_variations = []

    for variation in variations:

        resources = resources_lookup[variation['id']]
        attr_id = variation.get('attr_id')
        values = values_lookup[variation['id']]

        # create the variations object
        subvariations = []

        for resource in resources:
            ref_key = get_ref_key(resource)
            for value in values:
                # this creates a unique resource attribute value
                subvariations.append({(ref_key, resource.id, attr_id): {
                    'value': value,
                    'operator': variation.get('operator')
                }})

        all_variations.append(subvariations)

    # now, create cross product
    all_subscenarios = list(product(*all_variations))

    for ss in all_subscenarios:
        variations = {}
        for variation in ss:
            variations.update(variation)
        subscenarios.append({
            'parent_id': scenario.id,
            'variations': variations,
        })

    return subscenarios


def create_concurrent_subscenarios(network, template, scenario, scenario_type):
    return []


def get_resources_with_variation(network, template, scenario, variation):
    resource_scope = variation.get('resource_scope')
    resource_id = variation.get('ref_id')

    resources = []

    if resource_scope == 'resource':
        resource_type = variation.get('resource_type').lower()
        if resource_type == 'network':
            resources = [network]
        else:
            resources = [resource for resource in network[resource_type + 's'] if resource.id == resource_id]

    elif resource_scope == 'type':
        # get the template type
        ttype = list(filter(lambda x: x.id == resource_id), template.types)[0]
        ref_key = ttype['resource_type'].lower() + 's'
        resources = list(filter(lambda x: [t for t in x.types if t.id == resource_id], network[ref_key]))

    elif resource_scope == 'group':
        node_ids = []
        link_ids = []
        for item in scenario.resourcegroupitems:
            if item['group_id'] == resource_id:
                if item['ref_key'] == 'NODE':
                    node_ids.append(item['ref_id'])
                elif item['ref_key'] == 'LINK':
                    link_ids.append(item['ref_id'])
                # TODO: add network type?
        nodes = list(filter(lambda x: x.id in node_ids, network.nodes))
        links = list(filter(lambda x: x.id in link_ids, network.links))

        resources = nodes + links

    return resources
