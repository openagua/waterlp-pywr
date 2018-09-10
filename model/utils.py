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

    if not variations:
        return [{'parent_id': scenario.id, 'variations': {}}]

    concurrency = variations[0].get('concurrency')  # this implies we can only have one scope per scenario

    if concurrency == 'independent':
        return create_independent_subscenarios(network, template, scenario, scenario_type)
    elif concurrency == 'crosswise':
        return create_crosswise_subscenarios(network, template, scenario, scenario_type)
    elif concurrency == 'concurrent':
        return create_concurrent_subscenarios(network, template, scenario, scenario_type)


def create_independent_subscenarios(network, template, scenario, scenario_type):
    subscenarios = []
    # path = '{ref_key}/{ref_id}/{attr_id}/{val:.3f}'

    for variation in scenario.layout.get('variations', []):

        attr_id = variation.get('attr_id')
        resources = get_resources(network, template, scenario, variation)
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
        values_lookup[variation['id']] = get_variation_values(variation)
        resources_lookup[variation['id']] = get_resources(network, template, scenario, variation)

    def update_variations(obj=None):
        obj = obj or {}

        for i, variation in enumerate(variations):

            resources = resources_lookup[variation['id']]
            attr_id = variation.get('attr_id')
            values = values_lookup[variation['id']]

            for resource in resources:
                ref_key = get_ref_key(resource)
                for value in values:

                    # this creates a unique resource attribute value
                    obj[(ref_key, resource.id, attr_id)] = {
                        'value': value,
                        'operator': variation.get('operator')
                    }

                    # append to the variations set if on the last variation
                    if i == len(variations) - 1:
                        subscenarios.append({
                            'parent_id': scenario.id,
                            'variations': obj.copy()
                        })

                    # else continue on to the next variation set
                    else:
                        update_variations(obj)

    update_variations()

    return subscenarios


def create_concurrent_subscenarios(network, template, scenario, scenario_type):
    return []


def get_resources(network, template, scenario, variation):
    resource_scope = variation.get('resource_scope')
    ref_id = variation.get('ref_id')

    if resource_scope == 'resource':
        ref_key = variation.get('resource_type').lower() + 's'
        return [resource for resource in network[ref_key] if resource.id == ref_id]

    elif resource_scope == 'type':
        # get the template type
        ttype = list(filter(lambda x: x.id == ref_id), template.types)[0]
        ref_key = ttype.resource_type.lower() + 's'
        return list(filter(lambda x: [t for t in x.types if t.id == ref_id], network[ref_key]))

    elif resource_scope == 'group':
        node_ids = []
        link_ids = []
        for item in scenario.resourcegroupitems:
            if item['group_id'] == ref_id:
                if item['ref_key'] == 'NODE':
                    node_ids.append(item['ref_id'])
                elif item['ref_key'] == 'LINK':
                    link_ids.append(item['ref_id'])
                # TODO: add network type?
        nodes = list(filter(lambda x: x.id in node_ids, network.nodes))
        links = list(filter(lambda x: x.id in link_ids, network.links))

        return nodes + links
