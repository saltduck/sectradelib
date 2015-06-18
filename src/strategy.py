# coding:utf8
import logging
import threading
from time import sleep, time
from collections import defaultdict
from abc import ABCMeta, abstractmethod

import redisco

from .quoteservice import current_price
from .models.instrument import Instrument
from .models.order import Order
from .utils import logerror

logger = logging.getLogger(__name__)
rdb = redisco.get_client()

def wait_for_closed(order_idlist):
    """ 等待指定订单全部平仓完毕，超过30秒则放弃。返回是否成功。"""
    cnt = 0
    # copy and unique elements AND keep origin order_idlist immutable
    order_idlist = list(set(order_idlist))
    while order_idlist:
        sleep(1)
        if cnt >= 30:
            break
        cnt += 1
        for oid in order_idlist:
            order = Order.objects.get_by_id(oid)
            if order is None or order.is_closed():
                order_idlist.remove(oid)
    return not bool(order_idlist)


class BaseStrategy(object):
    __metaclass__ = ABCMeta

    def __init__(self, code, trader, app):
        self.code = str(code)
        self.trader = trader
        self.app = app
        self.orders = defaultdict(list)

    def check(self, instid):
        if not self.trader.can_trade():
            logger.debug('Can not trade!')
            return False
        if self.trader.close_lock:
            logger.debug('In close lock!')
            return False
        inst = Instrument.objects.get_by_id(self.trader.monitors[instid].instrument_id)
        if not inst.is_trading:
            logger.debug('Instrument {0} is not in trading!'.format(inst.secid))
            return False
        return True

    @logerror
    def run(self, instid, result=None):
        if self.check(instid):
            inst = Instrument.objects.get_by_id(self.trader.monitors[instid].instrument_id)
            self._do_strategy(inst, result)

    @logerror
    def _close_then_open(self, inst, direction, price, volume):
        to_be_closed = []
        for order in self.trader.opened_orders(instrument=inst, strategy_code=self.code):
            if order.can_close:
                logger.debug(u'先行平仓{0}'.format(order.sys_id))
                neworder = self.trader.close_order(order, strategy_code=str(self.code))
                if neworder:
                    to_be_closed.append(order.id)
        if not wait_for_closed(to_be_closed):
            logger.warning(u'平仓失败，放弃执行策略!')
            return
        volume = volume or self.get_max_volume(inst, direction)
        volume = min(volume, inst.max_order_volume)
        if volume < inst.min_order_volume:
            logger.warning(u'资金不足，无法下单!')
            return
        order = self.trader.open_order(inst, price, volume, direction, strategy_code=str(self.code))
        self.orders[inst.id].append(order.local_id)
        
    def close(self, inst):
        logger.info(u'策略{0}: 平仓{1}'.format(self.code, inst.name))
        for order in self.trader.opened_orders(instrument=inst, strategy_code=self.code):
            if order.can_close:
                self.trader.close_order(order, strategy_code=str(self.code))

    def buy(self, inst, price, volume):
        logger.info(u'策略{0}: 买进{1}'.format(self.code, inst.name))
        if inst.is_trading:
            threading.Thread(target=self._close_then_open, args=(inst, True, price, volume), name='BUY-'+self.trader.name).start()

    def sell(self, inst, price, volume=None):
        logger.info(u'策略{0}: 卖出{1}'.format(self.code, inst.name))
        if inst.is_trading:
            threading.Thread(target=self._close_then_open, args=(inst, False, price, volume), name='SELL-'+self.trader.name).start()

    @abstractmethod
    def _do_strategy(self, inst, result=None):
        """ 执行策略 """

    @abstractmethod
    def get_max_volume(self, inst, direction):
        """ 计算最大可下单手数 """


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
                order_idlist = self.trader.close_all()
                if wait_for_closed(order_idlist):
                    logger.info(u'全部平仓成功!')
                else:
                    logger.info(u'平仓失败！')
                self.trader.close_lock = False

    def run(self):
        while not self.trader.evt_stop.wait(self.interval):
            if self.trader.can_trade():
                self.check()


class CheckStopThread(threading.Thread):
    def __init__(self, trader):
        super(CheckStopThread, self).__init__(name='CKSTOP-'+trader.name)
        self.trader = trader

    @logerror
    def set_stopprice(self, instrument, price, offset):
        # 根据最新价格计算浮动止损价
        for order in self.trader.opened_orders(instrument=instrument):
            with self.trader.lock:
                order.set_stopprice(price, offset)

    @logerror
    def check(self, instrument, price):
        # 检查是否触及止损价
        to_be_closed = []
        for order in self.trader.opened_orders(instrument=instrument):
            direction = u''
            if order.opened_volume < 0 and price >= order.stopprice:
                direction = u'空'
            if order.opened_volume > 0 and price <= order.stopprice:
                direction = u'多'
            if direction:
                logger.warning(
                    u'合约{0}当前价格{1}触及{3}头止损价{2}，立即平仓!'.format(
                        order.instrument.name,
                        price,
                        order.stopprice,
                        direction,
                    )
                )
                self.trader.close_order(order)
                to_be_closed.append(order.id)
        if not wait_for_closed(to_be_closed):
            logger.warning(u'止损平仓失败，请检查原因!')

    def run(self):
        ps = rdb.pubsub()
        ps.subscribe('checkstop')
        logger.debug('CheckStopThread started...')
        while not self.trader.evt_stop.wait(0.1):
            item = ps.get_message()
            if item and item['type'] =='message':
                if not self.trader.can_trade():
                    logger.debug(u'交易程序未就绪')
                    continue
                instid = item['data']
                instrument = Instrument.objects.filter(secid=instid).first()
                if instrument is None:
                    logger.debug(u'非法合约代码: {0}'.format(instid))
                    continue
                monitor = self.trader.monitors.get(instid)
                if not monitor:
                    monitor = self.trader.monitors.get(instrument.product.prodid)
                if not monitor:
                    logger.debug(u'收到未监控合约{0}的checkstop消息, monitor={1}'.format(instid, self.trader.monitors))
                    continue
                cur_price = current_price(instid, None)
                self.set_stopprice(instrument, cur_price, monitor.offset)
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
        while not self.trader.evt_stop.wait(1):
            self.check()
