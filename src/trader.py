# coding:utf8
import logging
import threading
from datetime import datetime
import time
import json

import redisco

from .models.instrument import Instrument
from .models.account import Account, convert_currency
from .models.order import Order
from .utils import current_price, last_close_price

logger = logging.getLogger(__name__)
rdb = redisco.get_client()


class BaseTrader(object):
    def __init__(self, name, accountcode, currency, instrumentstr):
        self.name = name
        self.accountcode = accountcode
        if accountcode != 'simul':
            self.account = Account.objects.get_or_create(code=accountcode, default_currency=currency)
            if not self.account.last_trade_time:
                self.account.last_trade_time = datetime.utcnow()
            if not self.account.balances:
                self.account.deposit(0.0)
        self.max_balance = 0.0  # 本次运行（当天）最高资金余额
        self.monitors = {}
        self.offsets = {}
        for s in instrumentstr.split(','):
            try:
                symbol, offset_loss, offset_profit = s.split(':')
            except ValueError:
                offset_profit = 0
                try:
                    symbol, offset_loss = s.split(':')
                except ValueError:
                    offset_loss = 0
                    symbol = s
            symbol = symbol.strip()
            if symbol:
                self.offsets[symbol] = (float(offset_loss), float(offset_profit))
        logger.debug(str(self.offsets))

        self.close_lock = False
        self.is_logged = self.is_ready = False
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

    def user_login(self):
        pass

    def can_trade(self):
        return self.is_logged and self.is_ready

    def on_logon(self):
        self.is_logged = True

    def on_logout(self):
        self.is_logged = False

    def on_day_switch(self):
        self.max_balance = 0.0  # 最高资金余额每天清零

    def get_instrument_from_symbol(self, symbol):
        return Instrument.objects.filter(symbol=symbol).first()

    def set_monitors(self, publish=True):
        self.monitors = {}
        for symbol, offset in list(self.offsets.items()):
            symbol = symbol.strip()
            instrument = self.get_instrument_from_symbol(symbol)
            if publish:
                rdb.publish('mdmonitor', instrument.secid)  # Notify quoteservice
                rdb.publish('strategymonitor', json.dumps((symbol, instrument.id)))      # Notify strategy service
            self.monitors[symbol] = instrument
            logger.debug('add_instrument: {0}'.format(instrument))
        logger.debug('Set monitors to {0}'.format(self.monitors))

    def ready_for_trade(self):
        if not self.is_ready:
            logger.info('准备就绪，可以开始交易！')
        self.is_ready = True

    def on_account_changed(self):
        # 记录最高资金余额
        if self.account.balance > self.max_balance:
            self.max_balance = self.account.balance
        # 刷新显示
        if hasattr(self, 'infowin'):
            self.infowin.paint()

    def on_history_trade(self, execid, instid, orderid, local_id, direction, price, volume, exectime):
        with self.lock:
            inst = Instrument.objects.filter(secid=instid).first()
            order = Order.objects.filter(sys_id=orderid).first()
            if not order:
                logger.error('收到未知订单的历史成交记录, 订单号：{0}'.format(orderid))
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
                logger.warn('找不到本地订单号为{0}的订单'.format(local_id))
                return False
            logger.debug('instid=%s'%instid)
            order.on_new(orderid, instid, direction, price, volume, exectime)
            logger.info('<策略{0}>下单: {1}{2}仓 合约={3} 数量={4} 价格={5} 订单号={6}'.format(
                    order.strategy_code,
                    '开' if order.is_open else '平',
                    '多' if order.is_long == order.is_open else '空',
                    order.instrument.name,
                    volume,
                    price,
                    orderid,
                ))

    def on_reject(self, local_id, reason_code, reason_desc):
        with self.lock:
            logger.warning('订单(本地订单号：{0})被拒绝，原因：{1} {2}'.format(local_id, reason_code, reason_desc))
            order = Order.objects.filter(local_id=local_id).first()
            if order is None:
                logger.error('找不到订单号为{0}的订单'.format(local_id))
                return
            order.update_status(Order.OS_REJECTED)
            if not order.is_open:
                order.orig_order.update_status(Order.OS_FILLED)

    def on_cancel(self, local_id):
        with self.lock:
            order = Order.objects.filter(local_id=local_id).first()
            if not order:
                logger.error('收到未知订单的撤单回报，本地订单号：{0}'.format(local_id))
                return
            if order.is_open and order.status == Order.OS_FILLED:
                # 开仓单部成部撤特殊处理
                order.update_float_value('volume', order.filled_volume)
            else:
                order.update_status(Order.OS_CANCELED)
            if not order.is_open and order.orig_order.status == Order.OS_CLOSING:
                # 平仓单撤销后，恢复原开仓单状态
                order.orig_order.update_status(Order.OS_FILLED)
            logger.info('<{1}>订单(本地订单号：{0})已撤销'.format(local_id, order.strategy_code))
            return order

    def on_trade(self, execid, secid, orderid, price, volume, exectime, setstop=True):
        with self.lock:
            order = Order.objects.filter(sys_id=orderid).first()
            if order is None:
                logger.error('找不到订单号为{0}的订单'.format(orderid))
                return
            if order.is_open is None:
                logger.debug('订单(订单号：{0})无法交易，等待重试'.format(orderid))
                return False
            self.account.on_trade(order, execid, price, volume, exectime)
            if order.is_open and setstop:
                # 补仓或开新仓：按最新价设置止损价
                try:
                    offset = self.offsets[order.instrument.symbol]
                except KeyError:
                    try:
                        offset = self.offsets[order.instrument.product.prodid]
                    except AttributeError:
                        offset = [0.0, 0.0]
                if order.stop_profit_offset:
                    offset = list(offset)
                    offset[1] = order.stop_profit_offset
                    logger.debug('offset={0}'.format(offset))
                order.set_stopprice(price, *offset)

    def on_fullfilled(self, orderid):
        with self.lock:
            order = Order.objects.filter(sys_id=orderid).first()
            if order is None:
                logger.error(f'找不到订单号为{orderid}的订单')
                return
            order.volume = order.filled_volume
            order.save()

    def query_all_trades(self):
        """ 查询自从上次保存数据以来的所有成交历史 """
        self.query_history_trades(start_time=self.account.last_trade_time)

    def query_history_trades(self, start_time=None, end_time=None):
        """ 查询历史成交 """
        raise NotImplementedError

    def query_order_status(self, order):
        pass

    def open_order(self, inst, price, volume, direction, strategy_code=''):
        """ 开仓。返回新订单 or None。"""
        if not price:
            local_id = self.open_market_order(inst, volume, direction)
        else:
            local_id = self.open_limit_order(inst, price, volume, direction)
        if local_id:
            with self.lock:
                return self.account.create_order(local_id, inst, price, volume, True, strategy_code)

    def close_order(self, order, price=0.0, volume=None, strategy_code=''):
        """ 平仓。返回平仓订单 or None。"""
        if order.can_cancel:
            self.cancel_orders([order])
        if not order.can_close:
            logger.warning('订单{0}不允许平仓，状态为{1}'.format(order.local_id, order.status))
            return
        volume = volume or abs(order.opened_volume)
        if not price:
            local_id = self.close_market_order(order, volume)
        else:
            local_id = self.close_limit_order(order, price, volume)
        if local_id:
            with self.lock:
                order.update_status(Order.OS_CLOSING)
                return self.account.create_order(local_id, order.instrument, price, volume, False, strategy_code, order)

    def close_all(self, inst=None, limit_price_close=False):
        """ 平掉指定合约的所有浮仓。返回平仓单列表。"""
        orders = []
        for order in self.opened_orders(inst):
            if order.can_close:
                logger.debug('Closing Order {0}. filled_volume={1}, closed_volume={2}'.format(
                    order.sys_id, order.filled_volume, order.closed_volume))
                if limit_price_close:
                    neworder = self.close_order(order, last_close_price(order.instrument.secid))
                else:
                    neworder = self.close_order(order)
                if neworder:
                    orders.append(neworder)
        return orders

    def wait_for_closed(self, orders):
        """ 等待指定平仓单全部平仓完毕，超过30秒则撤单。
        返回是否全部成功平仓。"""
        if not orders:
            return True
        logger.debug('等待平仓单{0}执行成功...'.format(orders))
        orders = list(set(orders))
        for i in range(30):
            if not orders:
                break
            time.sleep(1)
            for order in orders:
                o = Order.objects.get_by_id(order.id)
                if o is None or o.is_closed() or o.orig_order.is_closed():
                    orders.remove(order)
        if orders:
            self.cancel_orders(orders)
        logger.debug('未成功平仓订单：{0}'.format(orders))
        return not bool(orders)

    def cancel_orders(self, orders):
        """ 撤单。返回成功撤销订单号列表。"""
        return []

    def cancel_order(self, order):
        return self.cancel_orders([order])
