from pywr.nodes import Domain, PiecewiseLink
from pywr.parameters import load_parameter

DEFAULT_RIVER_DOMAIN = Domain(name='river', color='#33CCFF')


class RiverDomainMixin(object):
    def __init__(self, *args, **kwargs):
        # if 'domain' not in kwargs:
        #     kwargs['domain'] = DEFAULT_RIVER_DOMAIN
        if 'color' not in kwargs:
            self.color = '#6ECFF6'  # blue
        super(RiverDomainMixin, self).__init__(*args, **kwargs)


class Hydropower(RiverDomainMixin, PiecewiseLink):
    """A river gauging station, with a minimum residual flow (MRF)
    """

    def __init__(self, *args, **kwargs):
        """Initialise a new Hydropower instance
        Parameters
        ----------
        mrf : float
            The minimum residual flow (MRF) at the gauge
        mrf_cost : float
            The cost of the route via the MRF
        cost : float
            The cost of the other (constrained) route
        max_flow : float
            The total capacity of the hydropower
        """
        # create keyword arguments for PiecewiseLink
        mrf = kwargs.pop('mrf', 0.0)
        cost = kwargs.pop('cost', 0.0)
        max_flow = kwargs.pop('max_flow', None)
        if max_flow is not None:
            max_flow -= mrf
        kwargs['cost'] = [kwargs.pop('mrf_cost', 0.0), cost]
        kwargs['max_flow'] = [mrf, max_flow]
        super(Hydropower, self).__init__(*args, **kwargs)

    def mrf():
        def fget(self):
            return self.sublinks[0].max_flow

        def fset(self, value):
            self.sublinks[0].max_flow = value

        return locals()

    mrf = property(**mrf())

    def mrf_cost():
        def fget(self):
            return self.sublinks[0].cost

        def fset(self, value):
            self.sublinks[0].cost = value

        return locals()

    mrf_cost = property(**mrf_cost())

    @classmethod
    def load(cls, data, model):
        mrf = load_parameter(model, data.pop("mrf"))
        mrf_cost = load_parameter(model, data.pop("mrf_cost"))
        cost = load_parameter(model, data.pop("cost", 0.0))
        max_flow = load_parameter(model, data.pop("max_flow", 0.0))
        del (data["type"])
        node = cls(model, mrf=mrf, mrf_cost=mrf_cost, cost=cost, max_flow=max_flow, **data)
        return node
