from pyomo.environ import AbstractModel, Set, Objective, Var, Param, Constraint, Reals, NonNegativeReals, minimize, \
    maximize, summation


# create the model
def create_model(name, nodes, links, types, ts_idx, params, blocks, debug_gain=False, debug_loss=False):
    m = AbstractModel(name=name)

    # SETS

    # basic sets
    m.Nodes = Set(initialize=nodes)  # nodes
    m.Links = Set(initialize=links)  # links
    m.TS = Set(initialize=ts_idx, ordered=True)  # time steps - initialize later?

    # all nodes directly upstream from a node
    def NodesIn_init(m, node):
        return [i for (i, j) in m.Links if j == node]

    m.NodesIn = Set(m.Nodes, initialize=NodesIn_init)

    # all nodes directly downstream from a node
    def NodesOut_init(m, node):
        return [k for (j, k) in m.Links if j == node]

    m.NodesOut = Set(m.Nodes, initialize=NodesOut_init)

    # sets (nodes or links) for each template type
    # TODO: make this more explicit; this method reduces transparency
    {setattr(m, k, Set(within=m.Nodes, initialize=v)) for k, v in types['node'].items()}
    {setattr(m, k, Set(within=m.Links, initialize=v)) for k, v in types['link'].items()}

    # sets for non-storage nodes
    m.Storage = m.Reservoir | m.Groundwater  # union
    m.NonStorage = m.Nodes - m.Storage  # difference
    m.DemandNodes = m.GeneralDemand | m.UrbanDemand | m.Hydropower | m.FlowRequirement | m.WaterTreatment  # we should eliminate differences
    m.NonJunction = m.Nodes - m.Junction

    # sets for links with channel capacity
    m.ConstrainedLink = m.Conveyance | m.DeliveryLink | m.Pipeline | m.Tunnel

    def nodeBlockLookup(i):
        return blocks['node'].get(i, [(0, 0)])

    def linkBlockLookup(i, j):
        return blocks['link'].get((i, j), [(0, 0)])

    # set - all blocks in each demand or reservoir node, and identify node-blocks
    def nodeBlockLookup_init(m):
        for i in m.NonJunction:
            return nodeBlockLookup(i)

    m.NodeBlockLookup = Set(dimen=2, initialize=nodeBlockLookup_init)

    # set - all blocks in each link
    def linkBlockLookup_init(m):
        for (i, j) in m.Links:
            return linkBlockLookup(i, j)

    m.LinkBlockLookup = Set(dimen=2, initialize=linkBlockLookup_init)

    # create node-block and link-block sets

    def NodeBlock(m):
        return [(j, b, sb) for j in (m.Storage | m.DemandNodes) for (b, sb) in nodeBlockLookup(j)]

    def LinkBlock(m):
        return [(i, j, b, sb) for i, j in m.Links for (b, sb) in linkBlockLookup(i, j)]

    m.NodeBlocks = Set(dimen=3, initialize=NodeBlock)
    m.LinkBlocks = Set(dimen=4, initialize=LinkBlock)

    # DECISION VARIABLES (all variables should be prepended with resource type)

    m.nodeDelivery = Var(m.DemandNodes * m.TS, domain=NonNegativeReals)  # delivery to demand nodes
    m.nodeStorageDelivery = Var(m.Storage * m.TS, domain=NonNegativeReals)  # delivery to storage nodes
    m.nodeDeliveryDB = Var(m.NodeBlocks * m.TS, domain=NonNegativeReals)  # delivery to demand nodes
    m.linkDelivery = Var(m.Links * m.TS, domain=NonNegativeReals)  # not valued yet; here as a placeholder
    m.linkDeliveryDB = Var(m.LinkBlocks * m.TS, domain=NonNegativeReals)
    m.nodeStorage = Var(m.Storage * m.TS, domain=NonNegativeReals)  # storage

    # m.nodeDemandDeficit = Var(m.NodeBlocks * m.TS, domain=NonNegativeReals, initialize=0.0)

    # m.nodeStorageDB = Var(m.Storage)
    # m.nodeFulfillmentDB = Var(m.NodeBlocks * m.TS, domain=NonNegativeReals) # Percent of delivery fulfilled (i.e., 1 - % shortage)

    m.nodeGain = Var(m.Nodes * m.TS, domain=Reals)  # gain (local inflows; can be net positive or negative)
    if debug_gain:
        m.debugGain = Var(m.Nodes * m.TS, domain=NonNegativeReals)
    if debug_loss:
        m.debugLoss = Var(m.Nodes * m.TS, domain=NonNegativeReals)
    m.nodeLoss = Var(m.Nodes * m.TS, domain=Reals)  # loss (local outflow; can be net positive or negative)
    m.nodeInflow = Var(m.Nodes * m.TS, domain=NonNegativeReals)  # total inflow to a node
    m.nodeOutflow = Var(m.Nodes * m.TS, domain=NonNegativeReals)  # total outflow from a node

    m.linkInflow = Var(m.Links * m.TS, domain=NonNegativeReals)  # total inflow to a link
    m.linkOutflow = Var(m.Links * m.TS, domain=NonNegativeReals)  # total outflow from a link

    m.nodeRelease = Var(m.Reservoir * m.TS, domain=NonNegativeReals)  # controlled release to a river
    m.nodeSpill = Var(m.Reservoir * m.TS, domain=NonNegativeReals)  # uncontrolled/undesired release to a river
    m.nodeExcess = Var((m.FlowRequirement | m.WaterTreatment | m.Hydropower) * m.TS, domain=NonNegativeReals)
    m.emptyStorage = Var(m.Reservoir * m.TS, domain=NonNegativeReals)  # empty storage space
    m.floodStorage = Var(m.Reservoir * m.TS, domain=NonNegativeReals)

    # variables to prevent infeasibilities
    m.virtualPrecipGain = Var(m.Reservoir * m.TS, domain=NonNegativeReals)  # allow reservoir to make up for excess evap
    m.groundwaterLoss = Var(m.Groundwater * m.TS, domain=NonNegativeReals)  # added to allow groundwater to overflow

    # PARAMETERS

    # TODO: replace this with explicit declarations
    for param in params.values():
        if param['is_var'] == 'N':
            initial_values = param.get('initial_values')  # initial values is used in expression execution
            expression = param.get('expression')
            if expression:
                exec(expression)

    m.nodeLocalGain = Param(m.Nodes, m.TS, default=0)  # placeholder
    m.nodeLocalLoss = Param(m.Nodes, m.TS, default=0)  # placeholder

    # parameters to convert priorities to values
    m.nodeValueDB = Param(m.NodeBlocks * m.TS, default=0, mutable=True)
    m.linkValueDB = Param(m.LinkBlocks * m.TS, default=0, mutable=True)

    # CONSTRAINTS

    # Constraint set: boundary conditions
    def LocalGain_rule(m, j, t):
        if j in m.Groundwater:
            '''Groundwater nodes can gain water from recharge'''
            gain = m.nodeNaturalRecharge[j, t]
        # elif j in m.Reservoir:
        # '''Reservoir nodes can gain water from local gains'''
        # gain = m.nodeLocalGain[j,t] # + m.nodeLocalPrecipitation[j,t]
        elif j in m.Catchment:
            '''Catchment nodes can gain water from runoff'''
            gain = m.nodeRunoff[j, t]
        else:
            '''Other nodes can gain water from local gains'''
            gain = m.nodeLocalGain[j, t]
        if debug_gain:
            return m.nodeGain[j, t] == gain + m.debugGain[j, t]
        else:
            return m.nodeGain[j, t] == gain

    m.LocalGain_constraint = Constraint(m.Nodes, m.TS, rule=LocalGain_rule)

    def LocalLoss_rule(m, j, t):
        if j in m.Reservoir:
            # excess evap should not cause infeasibility, so (expensive) virtualPrecepGain is subtracted from net evap
            loss = m.nodeNetEvaporation[j, t] - m.virtualPrecipGain[j, t]
            # loss = 0
        elif j in m.FlowRequirement | m.WaterTreatment | m.Hydropower:
            loss = 0
        elif j in m.DemandNodes:
            loss = m.nodeLocalLoss[j, t] + m.nodeDelivery[j, t] * m.nodeConsumptiveLoss[j, t] / 100
        elif j in m.Groundwater:
            loss = m.nodeLocalLoss[j, t] + m.groundwaterLoss[j, t]  # groundwater can disappear from the system
        else:
            loss = m.nodeLocalLoss[j, t]
        if debug_loss:
            return m.nodeLoss[j, t] == loss + m.debugLoss[j, t]
        else:
            return m.nodeLoss[j, t] == loss

    m.LocalLoss_constraint = Constraint(m.Nodes, m.TS, rule=LocalLoss_rule)

    def NodeInflow_definition(m, j, t):
        '''Node inflow is defined as the sum of all inflows'''
        return m.nodeInflow[j, t] == sum(m.linkOutflow[i, j, t] for i in m.NodesIn[j])

    m.NodeInflow_constraint = Constraint(m.Nodes, m.TS, rule=NodeInflow_definition)

    def NodeOutflow_definition(m, j, t):  # not to be confused with Outflow resources
        '''Node outflow is defined as the sum of all outflows (except for Outflow nodes, where water can leave the system)'''
        if j in m.OutflowNode:
            return Constraint.Skip  # no outflow constraint at outflow nodes
        else:
            return m.nodeOutflow[j, t] == sum(m.linkInflow[j, k, t] for k in m.NodesOut[j])

    m.NodeOutflow_constraint = Constraint(m.Nodes, m.TS, rule=NodeOutflow_definition)

    # Define deliveries. "Delivery" is defined as any amount of water going to a non-storage node--or water that is stored in a storage node--that is valued / prioritized"
    # Delivery comprises "delivery blocks", which correspond to demand blocks. Deliveries are a subset of physical water, such that actual storage and actual flows may be higher than deliveries.

    def NodeDelivery_definition(m, j, t):
        '''Deliveries comprise delivery blocks'''
        if j in m.Storage:
            return m.nodeStorageDelivery[j, t] == sum(m.nodeDeliveryDB[j, b, sb, t] for (b, sb) in nodeBlockLookup(j))
        elif m.DemandNodes:
            return m.nodeDelivery[j, t] == sum(m.nodeDeliveryDB[j, b, sb, t] for (b, sb) in nodeBlockLookup(j))
        else:
            return Constraint.Skip

    m.NodeStorageDelivery_definition = Constraint(m.Storage, m.TS, rule=NodeDelivery_definition)
    m.NodeDemandDelivery_definition = Constraint(m.DemandNodes, m.TS, rule=NodeDelivery_definition)

    def NodeDelivery_rule(m, j, t):
        '''Deliveries may not exceed physical conditions.'''
        if j in m.Storage:
            return m.nodeStorageDelivery[j, t] <= m.nodeStorage[j, t]
        elif j in m.FlowRequirement | m.WaterTreatment | m.Hydropower:
            return m.nodeDelivery[j, t] + m.nodeExcess[j, t] <= sum(m.linkOutflow[i, j, t] for i in m.NodesIn[j])
        elif j in m.DemandNodes:
            # deliveries to demand nodes (urban, ag, general) must equal actual inflows
            # note the use of local gains & losses: local sources such as precipitation can be included in deliveries
            # TODO: make this more sophisticated to account for more specific gains and losses (basically, everything except consumptive losses; this might be left to the user to add precip, etc. as part of a local gain function)
            # in the following, the assumption is that any water going to a demand node is accounted for as a delivery
            return m.nodeDelivery[j, t] == m.nodeInflow[j, t] + m.nodeLocalGain[j, t] - m.nodeLocalLoss[j, t]
        else:
            # delivery cannot exceed sum of inflows
            # TODO: update this to also include local gains and losses
            # return m.nodeDelivery[j, t] <= sum(m.linkOutflow[i, j, t] for i in m.NodesIn[j])
            return Constraint.Skip

    m.NodeDelivery_rule = Constraint(m.Nodes, m.TS, rule=NodeDelivery_rule)

    def NodeBlock_rule(m, j, b, sb, t):
        '''Delivery blocks cannot exceed their corresponding demand blocks.
        By extension, deliveries cannot exceed demands.
        '''
        if j in m.Storage:
            return m.nodeDeliveryDB[j, b, sb, t] <= m.nodeStorageDemand[j, b, sb, t]
        else:
            return m.nodeDeliveryDB[j, b, sb, t] <= m.nodeDemand[j, b, sb, t]

    m.NodeBlock_constraint = Constraint(m.NodeBlocks, m.TS, rule=NodeBlock_rule)

    # def DemandDeficit_rule(m, j, b, t):
    # '''Demand deficit definition'''
    # return m.nodeDemandDeficit[j, b, t] == m.nodeDemand[j, b, t] - m.nodeDeliveryDB[j, b, t]
    # m.NodeDemandDeficit_constraint = Constraint(m.NodeBlocks, m.TS, rule=DemandDeficit_rule)

    # def LinkBlock_rule(m, i, j, b, t):
    # '''Link flow blocks cannot exceed their corresponding demand blocks.'''
    # return m.linkDeliveryDB[i,j,b,t] <= m.linkDemand[i,j,b,t]
    # m.LinkBlock_constraint = Constraint(m.LinkBlocks, m.TS, rule=LinkBlock_rule)

    # Constraint set: Block mass balances

    def LinkMassBalance_rule(m, i, j, t):
        '''Define the relationship between link inflow and link outflow.'''
        # TODO: make this more sophisticated, with loss to groundwater
        return m.linkOutflow[i, j, t] == m.linkInflow[i, j, t] * (1 - m.linkLossFromSystem[i, j, t] / 100)

    m.LinkMassBalance_constraint = Constraint(m.Links, m.TS, rule=LinkMassBalance_rule)

    # def LinkDelivery_definition(m, i, j, t):
    # '''Water delivered via each link equals the sum of demand blocks for the link.'''
    # return m.linkDelivery[i,j,t] == sum(m.linkDeliveryDB[i,j,b,t] for b in m.LinkBlockLookup[i,j]) + m.linkFlowSurplus[i,j,t]
    # m.LinkBlockMassBalance = Constraint(m.River, m.TS, rule=LinkBlockMassBalance_rule)

    # general node mass balance
    def NodeMassBalance_rule(m, j, t):

        if j in m.Storage:  # this includes both reservoir and groundwater storage
            if t == m.TS.first():
                return m.nodeStorage[j, t] - m.nodeInitialStorage[j] == \
                       m.nodeGain[j, t] + m.nodeInflow[j, t] - m.nodeLoss[j, t] - m.nodeOutflow[j, t]
            else:
                return m.nodeStorage[j, t] - m.nodeStorage[j, m.TS.prev(t)] == \
                       m.nodeGain[j, t] + m.nodeInflow[j, t] - m.nodeLoss[j, t] - m.nodeOutflow[j, t]
        else:
            return m.nodeGain[j, t] + m.nodeInflow[j, t] == m.nodeLoss[j, t] + m.nodeOutflow[j, t]

    m.NodeMassBalance = Constraint(m.Nodes, m.TS, rule=NodeMassBalance_rule)

    def ReservoirRelease_definition(m, i, j, t):
        '''Reservoir release into a river (i.e., not including releases to conveyances)'''
        if i in m.Reservoir:
            return m.nodeRelease[i, t] + m.nodeSpill[i, t] == m.linkInflow[i, j, t]
        else:
            return Constraint.Skip

    m.ReservoirRelease = Constraint(m.River, m.TS, rule=ReservoirRelease_definition)

    def MaxReservoirRelease_rule(m, j, t):
        return m.nodeRelease[j, t] <= m.nodeMaximumOutflow[j, t]

    m.MaximumOutflow = Constraint(m.Reservoir, m.TS, rule=MaxReservoirRelease_rule)

    # def ExcessFlowRequirement_definition(m, j, t):
    #     return m.nodeInflow

    def EmptyStorage_definition(m, j, t):
        return m.emptyStorage[j, t] == m.nodeStorageCapacity[j, t] - m.nodeStorage[j, t]

    m.EmptyStorageDefinition = Constraint(m.Reservoir, m.TS, rule=EmptyStorage_definition)

    def FloodZone_definition(m, j, t):
        '''Stored flood is storage less delivery. By definition, storage equals delivery below the flood zone.'''
        return m.floodStorage[j, t] == m.nodeStorage[j, t] - m.nodeStorageDelivery[j, t]

    m.FloodZoneDefinition = Constraint(m.Reservoir, m.TS, rule=FloodZone_definition)

    # channel capacity
    def ChannelInflowCap_rule(m, i, j, t):
        if (i, j, t) in m.linkFlowCapacity and m.linkFlowCapacity[i, j, t].value >= 0:
            return m.linkInflow[i, j, t] <= m.linkFlowCapacity[i, j, t]
        else:
            return Constraint.Skip

    def ChannelOutflowCap_rule(m, i, j, t):
        if (i, j, t) in m.linkFlowCapacity and m.linkFlowCapacity[i, j, t].value >= 0:
            return m.linkOutflow[i, j, t] <= m.linkFlowCapacity[i, j, t]
        else:
            return Constraint.Skip

    m.ChannelInflowCapacity = Constraint(m.ConstrainedLink, m.TS, rule=ChannelInflowCap_rule)
    m.ChannelOutflowCapacity = Constraint(m.ConstrainedLink, m.TS, rule=ChannelOutflowCap_rule)

    # storage capacity
    def StorageBounds_rule(m, j, t):
        if j in m.Reservoir:
            return (m.nodeInactivePool[j, t], m.nodeStorage[j, t], m.nodeStorageCapacity[j, t])
        elif j in m.Groundwater:
            return (0, m.nodeStorage[j, t], m.nodeStorageCapacity[j, t])
        else:
            return None

    m.StorageBounds = Constraint(m.Storage, m.TS, rule=StorageBounds_rule)

    # OBJECTIVE FUNCTION

    def Objective_fn(m):
        # Link demand / value not yet implemented

        fn = summation(m.nodeValueDB, m.nodeDeliveryDB) \
                   - 1000 * summation(m.virtualPrecipGain) \
                   - 10 * summation(m.floodStorage) \
                   - 5 * summation(m.nodeSpill) \
                   - 1 * summation(m.emptyStorage)
        fn_debug_gain = - 1000 * summation(m.debugGain) if debug_gain else 0
        fn_debug_loss = - 1000 * summation(m.debugLoss) if debug_loss else 0

        return fn + fn_debug_gain + fn_debug_loss

    m.Objective = Objective(rule=Objective_fn, sense=maximize)

    return m
