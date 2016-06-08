# coding:utf8
import logging
import threading
from time import sleep, time
from collections import defaultdict
from abc import ABCMeta, abstractmethod
from datetime import datetime

import redisco

from .quoteservice import current_price
from .models.instrument import Instrument
from .models.order import Order
from .utils import logerror, exchange_time

logger = logging.getLogger(__name__)
rdb = redisco.get_client()


class BaseStrategy(object):
    def __init__(self, code, trader, app):
        self.code = str(code)
        self.trader = trader
        self.app = app
        self.orders = defaultdict(list)

    def check(self, symbol):
        if not self.trader.can_trade():
            logger.debug('Can not trade!')
            return False
        if self.trader.close_lock:
            logger.debug('In close lock!')
            return False
        inst = self.trader.monitors[symbol]
        if not inst.is_trading:
            logger.debug('Instrument {0} is not in trading!'.format(inst.secid))
            return False
        return inst
        
    @logerror
    def open_order(self, inst, price, volume, direction, delay=60, step=0.0, count=1, action=''):
        @logerror
        def work(order, delay, step, count, action):
            ok =False
            for i in range(count):
                if self.trader.evt_stop.wait(delay):
                    # system exit
                    break
                if not self.trader.is_simul:
                    order = Order.objects.get_by_id(order.id)
                if order.filled_volume == order.volume:
                    # Completed
                    ok = True
                    break
                if i == count-1:    # last loop
                    break
                if direction:
                    price = order.price + step
                else:
                    price = order.price - step
                volume = abs(order.volume) - abs(order.filled_volume)
                if not self.trader.cancel_order(order):
                    ok = True
                    break
                self.orders[inst.id].remove(order.local_id)
                neworder = self.trader.open_order(order.instrument, price, volume, order.is_long, strategy_code=self.code)
                self.on_canceL(order, neworder)
                if not neworder:                    
                    break
                order = neworder
                self.orders[inst.id].append(order.local_id)
            if not ok:
                if action == 'CANCEL':
                    if self.trader.cancel_order(order):
                        self.orders[inst.id].remove(order.local_id)
                elif action == 'MARKET':
                    order = self.trader.open_order(inst, 0.0, volume, direction, strategy_code=self.code)
                    if order:
                        self.orders[inst.id].append(order.local_id)
            return order
        order = self.trader.open_order(inst, price, volume, direction, strategy_code=self.code)
        if order:
            self.orders[inst.id].append(order.local_id)
            if not self.trader.is_simul and count > 0:
                threading.Thread(target=work, args=(order, delay, step, count, action)).start()
        return order

    def close(self, inst, price):
        logger.info(u'策略{0}: 平仓{1}'.format(self.code, inst.name))
        for order in self.trader.opened_orders(instrument=inst, strategy_code=self.code):
            if order.can_close:
                self.trader.close_order(order, price, strategy_code=str(self.code))

    def buy(self, inst, price, volume=None):
        logger.info(u'策略{0}: 买进{1}'.format(self.code, inst.name))
        if inst.is_trading:
            return self.open_order(inst, price, volume, True)

    def sell(self, inst, price, volume=None):
        logger.info(u'策略{0}: 卖出{1}'.format(self.code, inst.name))
        if inst.is_trading:
            return self.open_order(inst, price, volume, False)

    def on_cancel(self, order, neworder):
        pass


class CheckAvailableThread(threading.Thread):
    def __init__(self, trader, interval, reserve):
        super(CheckAvailableThread, self).__init__(name='CKAVAIL-'+trader.name)
        self.trader = trader
        self.interval = interval
        self.reserve = reserve

    @logerror
    def check(self):
        account = self.trader.account
        if account.available / account.balance < self.reserve / 100.0:
            logger.warning(u'资金不足，平掉全部浮仓!')
            self.trader.close_lock = True
            orders = self.trader.close_all()
            if not self.trader.wait_for_closed(orders):
                logger.info(u'平仓失败！')
            else:
                logger.info(u'全部平仓成功!')
            self.trader.close_lock = False

    def run(self):
        while not self.trader.evt_stop.wait(self.interval):
            if self.trader.can_trade():
                self.check()


class CheckStopThread(threading.Thread):
    def __init__(self, trader, limit_price_close=False):
        super(CheckStopThread, self).__init__(name='CKSTOP-'+trader.name)
        self.trader = trader
        self.limit_price_close = limit_price_close

    @logerror
    def set_stopprice(self, instrument, price, offset_loss, offset_profit=0.0):
        # 根据最新价格计算浮动止损价
        for order in self.trader.opened_orders(instrument=instrument):
            with self.trader.lock:
                order.set_stopprice(price, offset_loss, offset_profit)

    def close_order(self, order, price=0.0):
        return self.trader.close_order(order, price, strategy_code=order.strategy_code)

    @logerror
    def check(self, instrument, price):
        # 检查是否触及止损或止赢价
        to_be_closed = []
        for order in self.trader.opened_orders(instrument=instrument):
            direction = u''
            if order.stoploss:
                if order.opened_volume < 0 and price >= order.stoploss:
                    direction = u'空头止损'
                    stopprice = order.stoploss
                if order.opened_volume > 0 and price <= order.stoploss:
                    direction = u'多头止损'
                    stopprice = order.stoploss
            if order.stopprofit:
                if order.opened_volume < 0 and price <= order.stopprofit:
                    direction = u'空头止赢'
                    stopprice = order.stopprofit
                if order.opened_volume > 0 and price >= order.stopprofit:
                    direction = u'多头止赢'
                    stopprice = order.stopprofit
            if direction:
                logger.warning(
                    u'<策略{4}>合约{0}当前价格{1}触及订单{5}{3}价{2}，立即平仓!'.format(
                        order.instrument.name,
                        price,
                        stopprice,
                        direction,
                        order.strategy_code,
                        order.sys_id,
                    )
                )
                neworder = self.close_order(order)
                if neworder:
                    to_be_closed.append(neworder)
        if not self.trader.wait_for_closed(to_be_closed):
            logger.warning(u'止损(赢)平仓失败，请检查原因!')

    def run(self):
        ps = rdb.pubsub()
        ps.subscribe('checkstop')
        logger.debug('CheckStopThread started...')
        while not self.trader.evt_stop.wait(0.1):
            item = ps.get_message()
            if item and item['type'] =='message':
                if not self.trader.can_trade():
                    # logger.debug(u'交易程序未就绪')
                    continue
                instid = item['data']
                instrument = Instrument.objects.filter(secid=instid).first()
                if instrument is None:
                    logger.debug(u'非法合约代码: {0}'.format(instid))
                    continue
                offset = self.trader.offsets.get(instrument.symbol)
                if not offset:
                    try:
                        offset = self.trader.offsets.get(instrument.product.prodid)
                    except AttributeError:
                        offset = None
                if not offset:
                    #logger.debug(u'收到未监控合约{0}的checkstop消息, monitor={1}'.format(instid, self.trader.monitors))
                    continue
                cur_price = current_price(instid, None)
                self.set_stopprice(instrument, cur_price, *offset)
                self.check(instrument, cur_price)
        logger.debug('CheckStopThread exited.')


class CheckUntradedOrderThread(threading.Thread):
    def __init__(self, trader):
        super(CheckUntradedOrderThread, self).__init__(name='CkUTO-' + trader.name)
        self.trader = trader
    
    @logerror
    def check(self):
        for order in self.trader.untraded_orders():
            self.trader.query_order_status(order)

    def run(self):
        while not self.trader.evt_stop.wait(5):
            if self.trader.can_trade():
                self.check()


class CheckLimitOrderThread(threading.Thread):
    def __init__(self, trader, timeout, cancel_wait_time=0.2):
        super(CheckLimitOrderThread, self).__init__(name='CkLimOrd-'+trader.name)
        self.trader = trader
        self.timeout = timeout
        self.cancel_wait_time = cancel_wait_time

    @logerror
    def check(self):
        for order in self.trader.untraded_orders():
            if not order.price:
                # 市价单不用撤
                continue
            cur_exchange_time = exchange_time(order.instrument.exchangeid)
            delta =  cur_exchange_time - order.order_time
            if delta.total_seconds() >= self.timeout:
                logger.debug(u'订单时间: {0}, 交易所当前时间: {1}'.format(order.order_time, cur_exchange_time))
                self.trader.cancel_order(order)
                # 限价平仓单撤销后重下市价平仓单
                if not order.is_open:
                    sleep(self.cancel_wait_time)
                    order = Order.objects.get_by_id(order.id)
                    if order.status == Order.OS_CANCELED:
                        self.trader.close_order(order.orig_order, strategy_code=order.strategy_code)

    def run(self):
        while not self.trader.evt_stop.wait(1):
            if self.trader.can_trade():
                self.check()
