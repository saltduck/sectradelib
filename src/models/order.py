# coding:utf8
import logging
from redisco import models

from .instrument import Instrument
from ..utils import current_price
from .. import STRATEGIES

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
    
    def on_trade(self, price, volume, trade_time, exec_id, is_open):
        self.price = float(price)
        self.volume = float(volume)
        self.trade_time = trade_time
        self.exec_id = exec_id
        self.commission = self.order.instrument.calc_commission(price, volume, is_open)
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
            if self.order.instrument.indirect_quotation:
                self.profit += self.order.instrument.amount(orig_trade.price, vol) - self.order.instrument.amount(self.price, vol)
            else:
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
    order_time = models.DateTimeField()
    price = models.FloatField(indexed=False)
    volume = models.FloatField(indexed=False)
    status = models.IntegerField(default=OS_NONE)
    orig_order = models.ReferenceField('Order', related_name='close_orders')
    stop_profit_offset = models.FloatField(indexed=False, default=0.0)  # 止赢偏离值
    stoploss = models.FloatField(indexed=False, default=0.0)     # 止损价
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
        return self.status == Order.OS_FILLED

    @property
    def can_cancel(self):
        if self.status in (Order.OS_NONE, Order.OS_NEW):
            return True
        elif self.status == Order.OS_FILLED:
            if abs(self.filled_volume) < abs(self.volume):
                return True
        return False
    
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

    @property
    def strategy(self):
        return STRATEGIES.get(self.strategy_code)

    def delete(self, *args, **kwargs):
        for t in self.trades:
            t.delete()
        super(Order, self).delete(*args, **kwargs)

    def update_index_value(self, att, value):
        assert att in ('status', 'is_open', 'local_id', 'sys_id')
        pipeline = self.db.pipeline()
        # remove from old index
        indkey = self._index_key_for_attr_val(att, getattr(self, att))
        pipeline.srem(indkey, self.id)
        pipeline.srem(self.key()['_indices'], indkey)
        # add to new index
        # in version 0.1.4 there is a bug in self._add_to_index(att, value, pipeline):
        #      the val paramter doesnot work, it's ignored.
        # so i have to hardcode it as following
        t, index = self._index_key_for(att, value)
        if t == 'attribute':
            pipeline.sadd(index, self.id)
            pipeline.sadd(self.key()['_indices'], index)
        elif t == 'list':
            for i in index:
                pipeline.sadd(i, self.id)
                pipeline.sadd(self.key()['_indices'], i)
        elif t == 'sortedset':
            zindex, index = index
            pipeline.sadd(index, self.id)
            pipeline.sadd(self.key()['_indices'], index)
            descriptor = self.attributes[att]
            score = descriptor.typecast_for_storage(value)
            pipeline.zadd(zindex, self.id, score)
            pipeline.sadd(self.key()['_zindices'], zindex)
        # set db value
        pipeline.hset(self.key(), att, value)
        pipeline.execute()
        # set instance value
        setattr(self, att, value)

    def update_status(self, value):
        value = int(value)
        assert 0 <= value < 7
        logger.debug('update order {2} status from {0} to {1}'.format(getattr(self, 'status'), value, self.sys_id))
        self.update_index_value('status', value)

    def change_to_open_order(self):
        self.update_index_value('is_open', 1)

    def update_local_id(self, value):
        value = str(value)
        self.update_index_value('local_id', value)

    def update_sys_id(self, value):
        value = str(value)
        self.update_index_value('sys_id', value)

    def update_float_value(self, att, value):
        assert att in ('stoploss', 'stopprofit', 'stop_profit_offset', 'volume')
        value = float(value)
        self.db.hset(self.key(), att, value)
        setattr(self, att, value)

    def update_stopprice(self, stoploss=None, stopprofit=None):
        if stoploss is not None:
            self.update_float_value('stoploss', stoploss)
        if stopprofit is not None:
            self.update_float_value('stopprofit', stopprofit)

    def update_stop_profit_offset(self, value):
        self.update_float_value('stop_profit_offset', value)

    def margin(self, cur_price=None):
        cur_price = cur_price or self.cur_price
        return self.instrument.calc_margin(cur_price, self.opened_volume)

    def float_profit(self, cur_price=None):
        cur_price = cur_price or self.cur_price
        profit = self.instrument.amount(cur_price,  self.opened_volume) - self.opened_amount
        if self.instrument.indirect_quotation:
            profit *= -1
        return profit
    
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
        t.on_trade(price, volume, tradetime, execid, self.is_open)
        self.update_status(Order.OS_FILLED)
        logger.info(u'<策略{0}>成交回报: {1}{2}仓 合约={3} 价格={4} 数量={5}'.format(
                self.strategy_code,
                u'开' if self.is_open else u'平',
                u'多' if self.is_long == self.is_open else u'空',
                self.instrument.name,
                price,
                volume,
            ))
        return t

    def set_stopprice(self, price, offset_loss=0.0, offset_profit=0.0):
        # 静态止赢价
        if offset_profit and not self.stopprofit:
            if self.is_long:
                stopprofit = price + offset_profit
            else:
                stopprofit = price - offset_profit
            self.update_stopprice(stopprofit=stopprofit)
            logger.debug('Order {0} set stop profit price to {1}'.format(
                self.sys_id, self.stopprofit))
        # 浮动止损价
        if offset_loss:
            if self.is_long:
                stoploss = price - offset_loss
                if self.stoploss and stoploss <= self.stoploss:
                    return
            else:
                stoploss = price + offset_loss
                if self.stoploss and stoploss >= self.stoploss:
                    return
            self.update_stopprice(stoploss)
            logger.debug('Order {0} set stop loss price to {1}'.format(
                self.sys_id, self.stoploss))

    def on_close(self, trade):
        trade.on_close()
        if abs(self.orig_order.closed_volume) >= abs(self.orig_order.filled_volume):
            self.orig_order.update_status(Order.OS_CLOSED)
            logger.debug(u'订单{0}已全部平仓'.format(self.orig_order.sys_id))
        if (abs(self.closed_volume) >= abs(self.volume)) or (abs(self.closed_volume) >= abs(self.orig_order.filled_volume)):
            self.update_status(Order.OS_CLOSED)
            logger.debug(u'订单{0}已全部平仓'.format(self.sys_id))
        #elif self.orig_order.is_closed() and self.opened_volume != 0:
            # 平仓单手数大于原订单开仓手数，原订单全部平仓后，将平仓单剩余手数改为开仓单
            #self.change_to_open_order()
            #logger.debug(u'订单{0}转为开仓单'.format(self.sys_id))
