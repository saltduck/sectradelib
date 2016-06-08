from datetime import datetime

from nose.tools import eq_, with_setup

from ..models import Instrument, Account, Order
from ..strategy import CheckStopThread
from .utils import TestTrader

def setup_func():
    Instrument.objects.create(secid='XX1505', name='XX1505', symbol='XX1505', quoted_currency='CNY', multiplier=1.0)

def teardown_func():
    Instrument.objects.filter(secid='XX1505').first().delete()
    a = Account.objects.filter(code='test').first()
    for o in a.orders:
        o.delete()
    a.delete()

@with_setup(setup_func, teardown_func)
def test_only_stop_loss():
    trader = TestTrader('test', 'test', 'CNY', 'XX1505:100')
    trader.set_monitors()
    thread = CheckStopThread(trader)
    inst = Instrument.objects.filter(secid='XX1505').first()
    order1 = trader.open_order(inst, 0.0, 1, True, 'anna1')
    trader.on_new_order(order1.local_id, 'XX1505', 'ORDER1', True, 0.0, 1, datetime.now())
    trader.on_trade('EXEC1', 'XX1505', 'ORDER1', 5000, 1, datetime.now())
    order1 = Order.objects.get_by_id(order1.id)
    eq_(order1.stoploss, 4900)

    thread.set_stopprice(inst, 5100, 100)
    order1 = Order.objects.get_by_id(order1.id)
    eq_(order1.stoploss, 5000)
    
    order2 = trader.open_order(inst, 0.0, 1, True, 'anna2')
    trader.on_new_order(order2.local_id, 'XX1505', 'ORDER2', True, 0.0, 1, datetime.now())
    trader.on_trade('EXEC2', 'XX1505', 'ORDER2', 4980, 1, datetime.now())
    order2 = Order.objects.get_by_id(order2.id)
    eq_(order2.stoploss, 4880)
    thread.trader.account.db.delete('opened_orders:{0}:{1}:'.format(thread.trader.account.id, inst))
    thread.set_stopprice(inst, 4880, 100)
    order1 = Order.objects.get_by_id(order1.id)
    eq_(order1.stoploss, 5000)

    thread.set_stopprice(inst, 5050, 100)
    order1 = Order.objects.get_by_id(order1.id)
    eq_(order1.stoploss, 5000)
    order2 = Order.objects.get_by_id(order2.id)
    eq_(order2.stoploss, 4950)

    thread.check(inst, 4970)
    order1 = Order.objects.get_by_id(order1.id)
    eq_(order1.status, Order.OS_CLOSING)
    order2 = Order.objects.get_by_id(order2.id)
    eq_(order2.status, Order.OS_FILLED)
    
    thread.set_stopprice(inst, 5150, 100)
    order2 = Order.objects.get_by_id(order2.id)
    eq_(order2.stoploss, 5050)

@with_setup(setup_func, teardown_func)
def test_stop_loss_and_profit():
    trader = TestTrader('test', 'test', 'CNY', 'XX1505:100:300')
    trader.set_monitors()
    thread = CheckStopThread(trader)
    inst = Instrument.objects.filter(secid='XX1505').first()
    order1 = trader.open_order(inst, 0.0, 1, True, 'anna1')
    trader.on_new_order(order1.local_id, 'XX1505', 'ORDER1', True, 0.0, 1, datetime.now())
    trader.on_trade('EXEC1', 'XX1505', 'ORDER1', 5000, 1, datetime.now())
    order1 = Order.objects.get_by_id(order1.id)
    eq_(order1.stoploss, 4900)
    eq_(order1.stopprofit, 5300)

    thread.set_stopprice(inst, 5100, 100, 300)
    order1 = Order.objects.get_by_id(order1.id)
    eq_(order1.stoploss, 5000)
    eq_(order1.stopprofit, 5300)
    
    order2 = trader.open_order(inst, 0.0, 1, True, 'anna2')
    trader.on_new_order(order2.local_id, 'XX1505', 'ORDER2', True, 0.0, 1, datetime.now())
    trader.on_trade('EXEC2', 'XX1505', 'ORDER2', 4980, 1, datetime.now())
    order2 = Order.objects.get_by_id(order2.id)
    eq_(order2.stoploss, 4880)
    eq_(order2.stopprofit, 5280)
    thread.trader.account.db.delete('opened_orders:{0}:{1}:'.format(thread.trader.account.id, inst))
    thread.set_stopprice(inst, 4880, 100, 300)
    order1 = Order.objects.get_by_id(order1.id)
    eq_(order1.stoploss, 5000)
    eq_(order1.stopprofit, 5300)

    thread.set_stopprice(inst, 5050, 100, 300)
    order1 = Order.objects.get_by_id(order1.id)
    eq_(order1.stoploss, 5000)
    eq_(order1.stopprofit, 5300)
    order2 = Order.objects.get_by_id(order2.id)
    eq_(order2.stoploss, 4950)
    eq_(order2.stopprofit, 5280)
    
    thread.set_stopprice(inst, 5150, 100, 300)
    order1 = Order.objects.get_by_id(order1.id)
    eq_(order1.stoploss, 5050)
    eq_(order1.stopprofit, 5300)
    order2 = Order.objects.get_by_id(order2.id)
    eq_(order2.stoploss, 5050)
    eq_(order2.stopprofit, 5280)

    thread.check(inst, 5290)
    order1 = Order.objects.get_by_id(order1.id)
    eq_(order1.status, Order.OS_FILLED)
    order2 = Order.objects.get_by_id(order2.id)
    eq_(order2.status, Order.OS_CLOSING)
