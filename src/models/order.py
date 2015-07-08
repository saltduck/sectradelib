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
        return self.order.instrument.amount(self.price, self.volume)

    @property
    def opened_amount(self):
        return self.order.instrument.amount(self.price, self.opened_volume)
    
    def on_trade(self, price, volume, trade_time, exec_id):
        self.price = float(price)
        self.volume = float(volume)
        self.trade_time = trade_time
        self.exec_id = exec_id
        commission = abs(self.order.instrument.amount(price, volume)) * 0.000025
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
            self.profit += self.order.instrument.amount(self.price - orig_trade.price, vol)
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
    stoploss = models.FloatField(indexed=False)     # 止损价
    stopprofit = models.FloatField(indexed=False, default=0.0)   # 止赢价

    def __repr__(self):
        return u'<Order: {0.id}({0.instrument}:{0.opened_volume})>'.format(self)

    def is_closed(self):
        return self.status == Order.OS_CLOSED

    @property
    def currency(self):
        return self.instrument.quoted_currency

    @property
    def can_close(self):
        return self.status in (Order.OS_FILLED, Order.OS_CANCELED)

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
        return sum([trade.amount for trade in self.trades])
    
    @property
    def avg_fill_price(self):
        if self.filled_volume:
            if self.instrument.indirect_quotation:
                return self.filled_volume * self.instrument.multiplier / self.trade_amt
            else:
                return self.trade_amt / (self.filled_volume * self.instrument.multiplier)
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
        return self.instrument.amount(cur_price,  self.opened_volume) - self.opened_amount
    
    def on_new(self, orderid, instid, direction, price, volume, exectime):
        instrument = Instrument.objects.filter(secid=instid).first()
        #assert self.is_open is not None
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
        logger.info(u'<策略{0}>成交回报: {1}{2}仓 合约={3} 价格={4} 数量={5}'.format(
                self.strategy_code,
                u'开' if self.is_open else u'平',
                u'多' if self.is_long == self.is_open else u'空',
                self.instrument.name,
                price,
                volume,
            ))
        return t

    def set_stopprice(self, price, offset_loss, offset_profit=0.0):
        # 静态止赢价
        if offset_profit and not self.stopprofit:
            if self.is_long:
                self.stopprofit = price + offset_profit
            else:
                self.stopprofit = price - offset_profit
            assert self.is_valid(), self.errors
            logger.debug('Order {0} set stop profit price to {1}'.format(
                self.sys_id, self.stopprofit))
            self.save()
        # 浮动止损价
        if self.is_long:
            stoploss = price - offset_loss
            if self.stoploss and stoploss <= self.stoploss:
                return
        else:
            stoploss = price + offset_loss
            if self.stoploss and stoploss >= self.stoploss:
                return
        self.stoploss = float(stoploss)
        assert self.is_valid(), self.errors
        logger.debug('Order {0} set stop loss price to {1}'.format(
            self.sys_id, self.stoploss))
        self.save()

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
