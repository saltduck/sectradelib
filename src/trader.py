# coding:utf8
import logging
import threading
from datetime import datetime
import time

import redisco

from .models.instrument import Instrument
from .models.account import Account, convert_currency
from .models.order import Order, InstrumentEx

logger = logging.getLogger(__name__)
rdb = redisco.get_client()


class BaseTrader(object):
    def __init__(self, accountcode, currency, instrumentstr):
        self.accountcode = accountcode
        self.account = Account.objects.get_or_create(code=accountcode, default_currency=currency)
        if not self.account.last_trade_time:
            self.account.last_trade_time = datetime.utcnow()
        if not self.account.balances:
            self.account.deposit(10000.0)
        self.monitors = {}
        for s in instrumentstr.split(','):
            symbol, offset = s.split(':')
            self.monitors[symbol.strip()] = float(offset)

        self.close_lock = False
        self.is_logged = self.is_ready = self.runnable = False
        self.evt_stop = threading.Event()
    
    @property
    def available(self):
        return self.account.available

    @property
    def margins(self):
        return self.account.margins

    @property
    def float_profits(self):
        return self.account.float_profits

    @property
    def real_profits(self):
        return self.account.real_profits

    def opened_orders(self, *args, **kwargs):
        return self.account.opened_orders(*args, **kwargs)

    def combined_positions(self):
        return self.account.combined_positions()

    def stop(self):
        self.evt_stop.set()

    def add_instrument(self, symbol, offset):
        instrument = Instrument.objects.filter(symbol=symbol.strip()).first()
        rdb.publish('mdmonitor', instrument.secid)  # Notify quoteservice
        monitor = InstrumentEx.objects.get_or_create(account=self.account, instrument_id=instrument.id)
        if offset is not None:
            monitor.offset = offset
            monitor.save()
        self.monitors[instrument.secid] = monitor
        logger.debug(u'add_instrument: {0}'.format(monitor))

    def user_login(self):
        pass

    def can_trade(self):
        return self.is_logged and self.is_ready

    def on_logon(self):
        self.is_logged = True

    def on_logout(self):
        self.is_logged = False

    def set_monitors(self):
        _monitors = self.monitors.copy()
        self.monitors = {}
        for symbol, offset in _monitors.items():
            if isinstance(offset, InstrumentEx):
                offset = offset.offset
            self.add_instrument(symbol=symbol, offset=offset)

    def ready_for_trade(self):
        if not self.is_ready:
            logger.info(u'准备就绪，可以开始交易！')
        self.is_ready = True

    def on_account_changed(self):
        if hasattr(self, 'infowin'):
            self.infowin.paint()

    def on_history_trade(self, execid, instid, orderid, local_id, direction, price, volume, exectime):
        inst = Instrument.objects.filter(secid=instid).first()
        order = Order.objects.filter(sys_id=orderid).first()
        if not order:
            logger.error(u'收到未知订单的历史成交记录, OrderID={0}'.format(orderid))
            return
        order.local_id = local_id
        assert order.is_open is not None, order
        if not order.sys_id:
            order.on_new(orderid, instid, direction, price, volume, exectime)
        self.on_trade(execid, instid, orderid, price, volume, exectime)

    def on_new_order(self, local_id, instid, orderid, direction, price, volume, exectime):
        # check duplicate
        if Order.objects.filter(sys_id=orderid):
            return
        order = Order.objects.filter(local_id=local_id).first()
        if order is None:
            logger.warn(u'找不到LocalOrderID={0}的订单'.format(local_id))
            order = self.account.create_order(local_id)
        order.on_new(orderid, instid, direction, price, volume, exectime)

    def on_reject(self, local_id, reason_code, reason_desc):
        logger.warning(u'订单(ClOrdID={0})被拒绝，原因：{1} {2}'.format(local_id, reason_code, reason_desc))
        order = Order.objects.filter(local_id=local_id).first()
        if order is None:
            logger.error(u'找不到LocalOrderID={0}的订单'.format(local_id))
            return
        order.status = Order.OS_REJECTED
        order.save()

    def on_cancel(self, orderid):
        order = Order.objects.filter(local_id=orderid).first()
        if not order:
            logger.error(u'收到未知订单的撤单回报，OrderID={0}'.format(orderid))
            return
        order.status = Order.OS_CANCELED
        order.save()

    def on_trade(self, execid, secid, orderid, price, volume, exectime):
        order = Order.objects.filter(sys_id=orderid).first()
        if order is None:
            logger.error(u'找不到OrderID={0}的订单'.format(orderid))
            return
        while order.is_open is None:
            time.sleep(1)
            order = Order.objects.filter(local_id=orderid).first()
        self.account.on_trade(order, execid, price, volume, exectime)
        if order.is_open:
            # 补仓或开新仓：按最新价设置止损价
            try:
                monitor = self.monitors[order.instrument.secid]
            except KeyError:
                monitor = self.monitors[order.instrument.product.prodid]
            try:
                monitor.set_stopprice(price, order.is_long)
            except AttributeError:
                pass

    def query_all_trades(self):
        """ 查询自从上次保存数据以来的所有成交历史 """
        self.query_history_trades(start_time=self.account.last_trade_time)

    def query_history_trades(self, start_time=None, end_time=None):
        """ 查询历史成交 """
        raise NotImplementedError

    def open_order(self, inst, price, volume, direction):
        """ 开仓。返回本地订单号。"""
        if not price:
            return self.open_market_order(inst, volume, direction)
        else:
            return self.open_limit_order(inst, price, volume, direction)

    def close_order(self, order, price=0.0, volume = None):
        """ 平仓。返回本地订单号。"""
        volume = volume or abs(order.opened_volume)
        if not price:
            return self.close_market_order(order, volume)
        else:
            return self.close_limit_order(order, price, volume)

    def close_all(self, inst=None):
        orig_orders = []
        for order in self.opened_orders(inst):
            if order.can_close:
                logger.debug(u'Closing Order {0}. filled_volume={1}, closed_volume={2}'.format(
                    order.sys_id, order.filled_volume, order.closed_volume))
                Order.close(self, order)
                orig_orders.append(order.sys_id)
        return orig_orders
