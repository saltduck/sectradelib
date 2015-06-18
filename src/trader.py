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
        self.lock = threading.RLock()
    
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

    def untraded_orders(self, *args, **kwargs):
        return self.account.untraded_orders(*args, **kwargs)
        
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
        logger.debug(u'Set monitors to {0}'.format(self.monitors))

    def ready_for_trade(self):
        if not self.is_ready:
            logger.info(u'准备就绪，可以开始交易！')
        self.is_ready = True

    def on_account_changed(self):
        if hasattr(self, 'infowin'):
            self.infowin.paint()

    def on_history_trade(self, execid, instid, orderid, local_id, direction, price, volume, exectime):
        with self.lock:
            inst = Instrument.objects.filter(secid=instid).first()
            order = Order.objects.filter(sys_id=orderid).first()
            if not order:
                logger.error(u'收到未知订单的历史成交记录, 订单号：{0}'.format(orderid))
                return
            order.local_id = local_id
            assert order.is_open is not None, order
            if not order.sys_id:
                order.on_new(orderid, instid, direction, price, volume, exectime)
            self.on_trade(execid, instid, orderid, price, volume, exectime)

    def on_new_order(self, local_id, instid, orderid, direction, price, volume, exectime):
        with self.lock:
            # check duplicate
            if Order.objects.filter(sys_id=orderid):
                return
            order = Order.objects.filter(local_id=local_id).first()
            if order is None:
                logger.warn(u'找不到本地订单号为{0}的订单'.format(local_id))
                return False
            order.on_new(orderid, instid, direction, price, volume, exectime)

    def on_reject(self, local_id, reason_code, reason_desc):
        with self.lock:
            logger.warning(u'订单(本地订单号：{0})被拒绝，原因：{1} {2}'.format(local_id, reason_code, reason_desc))
            order = Order.objects.filter(local_id=local_id).first()
            if order is None:
                logger.error(u'找不到订单号为{0}的订单'.format(local_id))
                return
            order.status = Order.OS_REJECTED
            order.save()

    def on_cancel(self, local_id):
        with self.lock:
            order = Order.objects.filter(local_id=local_id).first()
            if not order:
                logger.error(u'收到未知订单的撤单回报，本地订单号：{0}'.format(local_id))
                return
            order.status = Order.OS_CANCELED
            order.save()

    def on_trade(self, execid, secid, orderid, price, volume, exectime):
        with self.lock:
            order = Order.objects.filter(sys_id=orderid).first()
            if order is None:
                logger.error(u'找不到订单号为{0}的订单'.format(orderid))
                return
            if order.is_open is None:
                logger.debug(u'订单(订单号：{0})无法交易，等待重试'.format(orderid))
                return False
            self.account.on_trade(order, execid, price, volume, exectime)
            if order.is_open:
                # 补仓或开新仓：按最新价设置止损价
                try:
                    monitor = self.monitors[order.instrument.secid]
                except KeyError:
                    monitor = self.monitors[order.instrument.product.prodid]
                order.set_stopprice(price, monitor.offset)

    def query_all_trades(self):
        """ 查询自从上次保存数据以来的所有成交历史 """
        self.query_history_trades(start_time=self.account.last_trade_time)

    def query_history_trades(self, start_time=None, end_time=None):
        """ 查询历史成交 """
        raise NotImplementedError

    def query_order_status(self, order):
        pass

    def open_order(self, inst, price, volume, direction, strategy_code=''):
        """ 开仓。返回新订单。"""
        with self.lock:
            if not price:
                local_id = self.open_market_order(inst, volume, direction)
            else:
                local_id = self.open_limit_order(inst, price, volume, direction)
            if local_id:
                return self.account.create_order(local_id, True, strategy_code)

    def close_order(self, order, price=0.0, volume=None, strategy_code=''):
        """ 平仓。返回平仓订单。"""
        with self.lock:
            volume = volume or abs(order.opened_volume)
            if not price:
                local_id = self.close_market_order(order, volume)
            else:
                local_id = self.close_limit_order(order, price, volume)
            if local_id:
                return self.account.create_order(local_id, False, strategy_code, order)

    def close_all(self, inst=None):
        with self.lock:
            orig_orders = []
            for order in self.opened_orders(inst):
                if order.can_close:
                    logger.debug(u'Closing Order {0}. filled_volume={1}, closed_volume={2}'.format(
                        order.sys_id, order.filled_volume, order.closed_volume))
                    self.close_order(order)
                    orig_orders.append(order.id)
        return orig_orders
