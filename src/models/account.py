# coding:utf8
import logging
from itertools import groupby
from operator import attrgetter

from redisco import models

from .instrument import Instrument
from .order import Order
from ..quoteservice import current_price

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
    raise RuntimeError(u'找不到货币{0}兑{1}的汇率'.format(from_ccy, to_ccy))


class Balance(models.Model):
    value = models.FloatField()
    currency = models.Attribute()
        
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

    @property
    def orders(self):
        return Order.objects.filter(account_id=self.id)

    def opened_orders(self, instrument=None, strategy_code=''):
        queryset = self.orders
        if instrument:
            queryset = queryset.filter(instrument_id=instrument.id)
        if strategy_code:
            queryset = queryset.filter(strategy_code=strategy_code)
        orders = list(queryset.filter(status=Order.OS_FILLED))
        queryset = queryset.filter(status=Order.OS_CANCELED)
        orders.extend([o for o in queryset if o.opened_volume != 0.0])
        return orders

    def untraded_orders(self, instrument=None, strategy_code=''):
        queryset = self.orders
        if instrument:
            queryset = queryset.filter(instrument_id=instrument.id)
        if strategy_code:
            queryset = queryset.filter(strategy_code=strategy_code)
        orders = list(queryset.filter(status=Order.OS_NEW))
        queryset = queryset.filter(status=Order.OS_FILLED)
        orders.extend([o for o in queryset if abs(o.filled_volume) < abs(o.volume)])
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
        msg = u'{3}：{0}{1}, 余额{2}{1}'.format(change, currency, balance.value, memo)
        if u'利润' in msg:
            logger.info(msg)
        else:
            logger.debug(msg)
        
    def deposit(self, quantity, currency=''):
        currency = currency or self.default_currency
        self.book(quantity, currency, u'转入资金')
        
    def set_balance(self, quantity, currency=''):
        currency = currency or self.default_currency
        balance = self.get_balance_object(currency)
        balance.value = float(quantity)
        assert balance.is_valid(), balance.errors
        balance.save()
        logger.debug(u'设置资金余额：{0}{1}'.format(quantity, currency))

    def set_available(self, available):
        self._available = available

    def create_order(self, local_order_id, is_open=None, strategy_code='', orig_order=None):
        assert is_open is None or is_open == (orig_order is None), (is_open, orig_order)
        neworder = Order.objects.filter(local_id=local_order_id).first()
        if not neworder:
            neworder = Order(local_id=local_order_id)
            logger.debug('NEWORDER local_id={0}'.format(neworder.local_id))
        neworder.update_attributes(account=self, is_open=is_open, strategy_code=strategy_code)
        if orig_order:
            neworder.orig_order = orig_order
            if not neworder.strategy_code:
                neworder.strategy_code = orig_order.strategy_code
        assert neworder.is_valid(), neworder.errors
        neworder.save()
        return neworder

    def on_trade(self, order, execid, price, volume, tradetime):
        trade = order.on_trade(price, volume, tradetime, execid)
        if not trade:
            return
        self.book(-trade.commission, order.currency, u'<策略{0}>收取手续费'.format(order.strategy_code))
        if not order.is_open:
            # 平仓
            order.on_close(trade)
            self.book(trade.profit, order.currency, u'<策略{0}>获取利润'.format(order.strategy_code))
            self.real_profits += trade.profit
        if not self.last_trade_time or self.last_trade_time < tradetime:
            self.last_trade_time = tradetime
            self.save()
