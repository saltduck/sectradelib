# coding:utf8
import logging
from redisco import models

from .instrument import Instrument
from ..quoteservice import current_price

logger = logging.getLogger(__name__)


class Trade(models.Model):
    exec_id = models.Attribute(required=True)
    order = models.ReferenceField('Order')
    trade_time = models.DateTimeField(required=True)
    price = models.FloatField(required=True, indexed=False)
    volume = models.FloatField(required=True, indexed=False)
    closed_volume = models.FloatField(required=True, default=0.0, indexed=False)
    commission = models.FloatField(required=True, default=0.0, indexed=False)
    profit = models.FloatField(required=True, default=0.0, indexed=False)

    @property
    def opened_volume(self):
        return self.volume - self.closed_volume

    @property
    def amount(self):
        return self.price * self.volume * self.order.instrument.multiplier

    @property
    def opened_amount(self):
        return self.price * self.opened_volume * self.order.instrument.multiplier
    
    def on_trade(self, price, volume, trade_time, exec_id):
        self.price = float(price)
        self.volume = float(volume)
        self.trade_time = trade_time
        self.exec_id = exec_id
        commission = abs(price * volume * self.order.instrument.multiplier * 0.000025)
        ndigits = 0 if self.order.instrument.quoted_currency == 'JPY' else 2
        commission = round(commission, ndigits)
        self.commission = commission
        assert self.is_valid(), self.errors
        self.save()

    def on_close(self):
        for orig_trade in self.order.orig_order.trades:
            if abs(self.closed_volume) >= abs(self.volume):
                break
            if orig_trade.opened_volume == 0.0:
                continue
            if abs(orig_trade.opened_volume) < abs(self.opened_volume):
                vol = orig_trade.opened_volume
            else:
                vol = -self.opened_volume
            logger.debug('Trade {0} against {1} close volume={2}'.format(self.exec_id, orig_trade.exec_id, vol))
            self.closed_volume -= vol
            orig_trade.closed_volume += vol
            self.profit += (self.price - orig_trade.price) * vol * self.order.instrument.multiplier
            assert orig_trade.is_valid(), orig_trade.errors
            orig_trade.save()
        assert self.is_valid(), self.errors
        self.save()


class Order(models.Model):
    OS_NONE, OS_NEW, OS_CANCELED, OS_FILLED, OS_CLOSING, OS_CLOSED, OS_REJECTED = range(7)
    account = models.ReferenceField('Account')
    local_id = models.Attribute()
    sys_id = models.Attribute(default='')
    strategy_code = models.Attribute(default='')
    instrument = models.ReferenceField(Instrument)
    is_long = models.BooleanField(indexed=False)
    is_open = models.BooleanField(indexed=True)
    order_time = models.DateTimeField(indexed=False)
    price = models.FloatField(indexed=False)
    volume = models.FloatField(indexed=False)
    status = models.IntegerField(default=OS_NONE)
    orig_order = models.ReferenceField('Order')

    def __repr__(self):
        return u'<Order: {0.id}({0.instrument}:{0.opened_volume})>'.format(self)

    def is_closed(self):
        return self.status == Order.OS_CLOSED

    @property
    def currency(self):
        return self.instrument.quoted_currency

    @property
    def can_close(self):
        return self.status == Order.OS_FILLED

    @property
    def trades(self):
        return Trade.objects.filter(order_id=self.id).order('trade_time')
    
    @property
    def filled_volume(self):
        return sum([trade.volume for trade in self.trades])

    @property
    def closed_volume(self):
        return sum([trade.closed_volume for trade in self.trades])
        
    @property
    def opened_volume(self):
        """ 剩余开仓量 """
        return sum([trade.opened_volume for trade in self.trades])

    @property
    def opened_amount(self):
        return sum([trade.opened_amount for trade in self.trades])
    
    @property
    def commission(self):
        return sum([trade.commission for trade in self.trades])

    @property
    def real_profit(self):
        return sum([trade.profit for trade in self.trades])    

    @property
    def trade_amt(self):
        return sum([trade.price * trade.volume * self.instrument.multiplier for trade in self.trades])
    
    @property
    def avg_fill_price(self):
        if self.filled_volume:
            return self.trade_amt / self.filled_volume / self.instrument.multiplier
        return None

    @property
    def cur_price(self):
        return current_price(self.instrument.secid, self.opened_volume > 0)

    def delete(self, *args, **kwargs):
        for t in self.trades:
            t.delete()
        super(Order, self).delete(*args, **kwargs)

    def margin(self, cur_price=None):
        cur_price = cur_price or self.cur_price
        return self.instrument.calc_margin(cur_price, self.opened_volume)

    def float_profit(self, cur_price=None):
        cur_price = cur_price or self.cur_price
        return cur_price * self.opened_volume * self.instrument.multiplier - self.opened_amount
    
    @classmethod
    def open(cls, trader, inst, price, volume, direction, strategy_code=''):
        local_order_id = trader.open_order(inst, price, volume, direction)
        return trader.account.create_order(local_order_id, True, strategy_code)
    
    @classmethod
    def close(cls, trader, orig_order, strategy_code=''):
        local_order_id = trader.close_order(orig_order)
        if local_order_id:
            #orig_order.status = Order.OS_CLOSING
            #orig_order.save()
            return trader.account.create_order(local_order_id, False, strategy_code, orig_order)

    def on_new(self, orderid, instid, direction, price, volume, exectime):
        instrument = Instrument.objects.filter(secid=instid).first()
        assert self.is_open is not None
        self.sys_id = orderid
        self.instrument = instrument
        self.is_long = direction
        self.price = float(price)
        self.volume = float(volume)
        self.order_time = exectime
        self.status = Order.OS_NEW
        assert self.is_valid(), self.errors
        self.save()

    def on_trade(self, price, volume, tradetime, execid):
        assert self.is_open is not None
        # check duplicate trade
        if Trade.objects.filter(exec_id=execid):
            logger.debug(u'EXECID {0} 已经存在!'.format(execid))
            return False
        if not self.is_long:
            volume = -volume
        t = Trade(order=self)
        t.on_trade(price, volume, tradetime, execid)
        self.status = Order.OS_FILLED
        assert self.is_valid(), self.errors
        self.save()
        return t

    def on_close(self, trade):
        trade.on_close()
        if abs(self.orig_order.closed_volume) >= abs(self.orig_order.filled_volume):
            self.orig_order.status = Order.OS_CLOSED
            assert self.orig_order.is_valid(), self.orig_order.errors
            self.orig_order.save()
            logger.debug(u'订单{0}已全部平仓'.format(self.orig_order.sys_id))
        if abs(self.closed_volume) >= abs(self.filled_volume):
            self.status = Order.OS_CLOSED
            assert self.is_valid(), self.errors
            self.save()
        elif self.orig_order.is_closed():
            # 平仓单手数大于原订单开仓手数，原订单全部平仓后，将平仓单剩余手数改为开仓单
            self.is_open = True
            assert self.is_valid(), self.errors
            self.save()


class InstrumentEx(models.Model):
    account = models.ReferenceField('Account')
    instrument = models.ReferenceField(Instrument)
    offset = models.FloatField(default=0.0, indexed=False)
    stop_price_long = models.FloatField(default=0.0, indexed=False)
    stop_price_short = models.FloatField(default=99999999.0, indexed=False)

    def set_stopprice(self, tradeprice, direction):
        if direction:
            self.stop_price_long = tradeprice - self.offset
            logger.debug('Set stop price to {0}'.format(self.stop_price_long))
        else:
            self.stop_price_short = tradeprice + self.offset
            logger.debug('Set stop price to {0}'.format(self.stop_price_short))
        assert self.is_valid(), self.errors
        self.save()
