# coding:utf8
import logging
import json
from itertools import groupby
from operator import attrgetter

from redisco import models

from .instrument import Instrument
from .order import Order
from ..utils import current_price

logger = logging.getLogger(__name__)


def convert_currency(value, from_ccy, to_ccy):
    if from_ccy == to_ccy:
        return value
    secid = Instrument.symbol2id('{0}/{1}'.format(from_ccy, to_ccy))
    if secid:
        return value * current_price(secid, False)
    secid = Instrument.symbol2id('{1}/{0}'.format(from_ccy, to_ccy))
    if secid:
        return value / current_price(secid, True)
    raise RuntimeError('找不到货币{0}兑{1}的汇率'.format(from_ccy, to_ccy))


class Balance(models.Model):
    currency = models.Attribute()
    # 可用资金
    value = models.FloatField(indexed=False)
    # 冻结资金
    hold = models.FloatField(indexed=False)
    # 总余额
    balance = models.FloatField(indexed=False)

    def convert_to(self, to_ccy):
        return convert_currency(self.value, self.currency, to_ccy)


class Account(models.Model):
    code = models.Attribute(required=True)
    default_currency = models.Attribute(required=True, indexed=False, default='USD')
    last_trade_time = models.DateTimeField(indexed=False)
    balances = models.ListField(Balance, indexed=False)

    def __init__(self, *args, **kwargs):
        super(Account, self).__init__(*args, **kwargs)
        self.real_profits = 0.0
    
    def __repr__(self):
        return f"<Account: {self.id}({self.code})>"

    @property
    def orders(self):
        return Order.objects.filter(account_id=self.id)

    def opened_orders(self, instrument=None, strategy_code=''):
        key = 'opened_orders:{0}:{1}:{2}'.format(self.id, instrument, strategy_code)
        cached = self.db.get(key)
        if cached:
            return [Order.objects.get_by_id(oid) for oid in json.loads(cached)]
        else:
            queryset = self.orders
            if instrument:
                queryset = queryset.filter(instrument_id=instrument.id)
            if strategy_code:
                queryset = queryset.filter(strategy_code=strategy_code)
            orders = list(queryset.filter(status=Order.OS_FILLED))
            orders.extend(list(queryset.filter(status=Order.OS_CLOSING)))
            cached = json.dumps([o.id for o in orders])
            self.db.setex(key, cached, 1)
            return orders

    def untraded_orders(self, instrument=None, strategy_code=''):
        queryset = self.orders
        if instrument:
            queryset = queryset.filter(instrument_id=instrument.id)
        if strategy_code:
            queryset = queryset.filter(strategy_code=strategy_code)
        orders = list(queryset.zfilter(status__in=(Order.OS_NONE, Order.OS_NEW)))
        queryset = queryset.filter(status=Order.OS_FILLED)
        orders.extend([o for o in queryset if abs(o.filled_volume) < abs(o.volume) - o.instrument.size_increment])
        return orders

    def combined_positions(self):
        qs = sorted(self.opened_orders(), key=attrgetter('instrument_id'))
        groups = groupby(qs, attrgetter('instrument'))
        for inst, orders in groups:
            yield inst, sum([o.opened_volume for o in orders])

    @property
    def balance(self):
        return sum([b.convert_to(self.default_currency) for b in self.balances])
    
    @property
    def available(self):
        if getattr(self, '_available', None) is None:
            return self.balance - self.margins + self.float_profits
        return self._available

    @property
    def margins(self):
        return sum([convert_currency(o.margin(), o.currency, self.default_currency) for o in self.opened_orders()])

    @property
    def float_profits(self):
        return sum([convert_currency(o.float_profit(), o.currency, self.default_currency) for o in self.opened_orders()])

    def margin_account(self, instrument):
        return MarginAccount.objects.filter(account_id=self.id, instrument_id=instrument.id).first()

    def open_orders(self, strategy_code=''):
        queryset = self.orders.filter(is_open=True)
        if strategy_code:
            queryset = queryset.filter(strategy_code=strategy_code)
        return queryset

    def balance_in(self, ccy):
        return self.get_balance_object(ccy).value

    def get_balance_object(self, currency):
        try:
            balance = [b for b in self.balances if b.currency==currency][0]
        except IndexError:
            balance = Balance(currency=currency, value=0.0)
            balance.save()
            self.balances.append(balance)
            self.save()
        return balance

    def book(self, change, currency, memo):
        balance = self.get_balance_object(currency)
        balance.value += float(change)
        assert balance.is_valid(), balance.errors
        balance.save()
        msg = '{3}：{0}{1}, 余额{2}{1}'.format(change, currency, balance.value, memo)
        if '利润' in msg:
            logger.info(msg)
        else:
            logger.debug(msg)
        
    def deposit(self, quantity, currency=''):
        currency = currency or self.default_currency
        self.book(quantity, currency, '转入资金')
        
    def set_balance(self, quantity, currency=''):
        currency = currency or self.default_currency
        balance = self.get_balance_object(currency)
        balance.value = float(quantity)
        assert balance.is_valid(), balance.errors
        balance.save()
        #logger.debug(f'设置可用资金额：{quantity}{currency}')

    def set_available(self, available):
        self._available = available

    def create_order(self, local_order_id, inst=None, price=None, volume=None, is_open=None, strategy_code='', orig_order=None):
        assert is_open is None or is_open == (orig_order is None), (is_open, orig_order)
        neworder = Order.objects.filter(local_id=local_order_id).first()
        if not neworder:
            neworder = Order(local_id=local_order_id, instrument=inst)
            try:
                neworder.price = float(price)
            except:
                pass
            try:
                neworder.volume = float(volume)
            except:
                pass 
        neworder.update_attributes(account=self, is_open=is_open, strategy_code=strategy_code)
        if orig_order:
            neworder.orig_order = orig_order
            if not neworder.strategy_code:
                neworder.strategy_code = orig_order.strategy_code
        assert neworder.is_valid(), neworder.errors
        neworder.save()
        logger.debug('NEWORDER local_id={0}'.format(neworder.local_id))
        return neworder

    def on_trade(self, order, execid, price, volume, tradetime):
        trade = order.on_trade(price, volume, tradetime, execid)
        if not trade:
            return
        self.book(-trade.commission, order.currency, '<策略{0}>收取手续费'.format(order.strategy_code))
        if not order.is_open:
            # 平仓
            order.on_close(trade)
            self.book(trade.profit, order.currency, '<策略{0}>获取利润'.format(order.strategy_code))
            self.real_profits += trade.profit
        if not self.last_trade_time or self.last_trade_time < tradetime:
            self.last_trade_time = tradetime
            self.save()


class MarginBalance(Balance):
    # 借币余额
    borrowed = models.FloatField(indexed=False)
    # 借币利息余额
    lending_fee = models.FloatField(indexed=False)


class MarginAccount(models.Model):
    account = models.ReferenceField(Account, indexed=True)
    instrument = models.ReferenceField(Instrument, indexed=True)
    maint_ratio = models.FloatField(indexed=False, default=0.0)
    liquidation_price = models.FloatField(indexed=False, default=0.0)
    base_balance = models.ReferenceField(MarginBalance)
    quoted_balance = models.ReferenceField(MarginBalance)

    def base_available(self):
        return self.base_balance.value

    def quoted_available(self):
        return self.quoted_balance.value

    def available(self, currency):
        if currency == self.instrument.base_currency:
            return self.base_available()
        if currency == self.instrument.quoted_currency:
            return self.quoted_available()
