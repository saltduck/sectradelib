import itertools

from ..models import Order
from ..trader import BaseTrader


count = itertools.count()


class TestTrader(BaseTrader):
    def open_market_order(self, inst, volume, direction):
        return 'LOCAL{0}'.format(count.next())
    def close_market_order(self, order, volume):
        order.status = Order.OS_CLOSED
        order.save()
        return 'LOCAL{0}'.format(count.next())
    def wait_for_closed(self, orders):
        return True
    
