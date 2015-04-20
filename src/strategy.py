# coding:utf8
import logging
import threading
from time import sleep, time
from collections import defaultdict

import redisco

from .quoteservice import current_price
from .models.instrument import Instrument
from .models.order import Order

logger = logging.getLogger(__name__)
rdb = redisco.get_client()

def wait_for_closed(order_idlist):
    # copy and unique elements AND keep origin order_idlist immutable
    order_idlist = list(set(order_idlist))
    while order_idlist:
        for oid in order_idlist:
            order = Order.objects.get_by_id(oid)
            if order is None or order.is_closed():
                order_idlist.remove(oid)
        sleep(0.1)


class BaseStrategy(object):
    def __init__(self, code, trader, app):
        self.code = code
        self.trader = trader
        self.app = app
        self.orders = defaultdict(list)

    def run(self, instid):
        if not self.trader.can_trade():
            logger.debug('Can not trade!')
            return
        if self.trader.close_lock:
            logger.debug('In close lock!')
            return
        inst = Instrument.objects.get_by_id(self.trader.monitors[instid].instrument_id)
        if not inst.is_trading:
            logger.debug('Instrument {0} is not in trading!'.format(inst.secid))
            return
        self._do_strategy(inst)

    def _close_then_open(self, inst, direction, price, volume):
        to_be_closed = []
        for order in self.trader.opened_orders(instrument=inst, strategy_code=self.code):
            if order.can_close:
                logger.debug(u'先行平仓{0}'.format(order.sys_id))
                neworder = Order.close(self.trader, order, strategy_code=str(self.code))
                if neworder:
                    to_be_closed.append(order.id)
        wait_for_closed(to_be_closed)
        volume = volume or self.get_max_volume(inst, direction)
        volume = min(volume, inst.max_order_volume)
        if volume < inst.min_order_volume:
            logger.warning(u'资金不足，无法下单!')
            return
        order = Order.open(self.trader, inst, price, volume, direction, strategy_code=str(self.code))
        self.orders[inst.id].append(order.local_id)
        
    def close(self, inst):
        logger.info(u'策略{0}: 平仓{1}'.format(self.code, inst.name))
        for order in self.trader.opened_orders(instrument=inst, strategy_code=self.code):
            if order.can_close:
                Order.close(self.trader, order, strategy_code=str(self.code))

    def buy(self, inst, price, volume):
        logger.info(u'策略{0}: 买进{1}'.format(self.code, inst.name))
        if inst.is_trading:
            threading.Thread(target=self._close_then_open, args=(inst, True, price, volume)).start()

    def sell(self, inst, price, volume=None):
        logger.info(u'策略{0}: 卖出{1}'.format(self.code, inst.name))
        if inst.is_trading:
            threading.Thread(target=self._close_then_open, args=(inst, False, price, volume)).start()

    def _do_strategy(self, inst):
        raise NotImplementedError

    def get_max_volume(self, inst, direction):
        raise NotImplementedError


class CheckAvailableThread(threading.Thread):
    def __init__(self, trader, interval, reserve):
        super(CheckAvailableThread, self).__init__(name='CheckAvailable')
        self.trader = trader
        self.interval = interval
        self.reserve = reserve

    def run(self):
        account = self.trader.account
        while not self.trader.evt_stop.wait(self.interval):
            if not self.trader.can_trade():
                continue
            if account.available / account.balance < self.reserve / 100.0:
                logger.warning(u'资金不足，平掉全部浮仓!')
                self.trader.close_lock = True
                order_idlist = self.trader.close_all()
                wait_for_closed(order_idlist)
                self.trader.close_lock = False
                logger.info(u'全部平仓成功!')


class CheckStopThread(threading.Thread):
    def __init__(self, trader):
        super(CheckStopThread, self).__init__(name='CheckStop')
        self.trader = trader

    def run(self):
        ps = rdb.pubsub()
        ps.subscribe('checkstop')
        while not self.trader.evt_stop.wait(0.1):
            if not self.trader.can_trade():
                continue
            item = ps.get_message()
            if item and item['type'] =='message':
                instid = item['data']
                b_price = current_price(instid, True)
                s_price = current_price(instid, False)
                monitor = self.trader.monitors.get(instid)
                if not monitor:
                    continue
                for order in self.trader.opened_orders(instrument=monitor.instrument):
                    if order.opened_volume < 0 and s_price >= monitor.stop_price_short:
                        logger.warning(u'合约{0}当前价格{1}触及空头止损价{2}，立即平仓!'.format(order.instrument.name, s_price, monitor.stop_price_short))
                        Order.close(self.trader, order)
                    if order.opened_volume > 0 and b_price <= monitor.stop_price_long:
                        logger.warning(u'合约{0}当前价格{1}触及多头止损价{2}，立即平仓!'.format(order.instrument.name, b_price, monitor.stop_price_long))
                        Order.close(self.trader, order)
