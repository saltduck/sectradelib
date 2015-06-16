from datetime import datetime
import itertools

from nose.tools import eq_

from sectradelib.models import Instrument, Account, Order
from sectradelib.trader import BaseTrader
from sectradelib.strategy import CheckStopThread

count = itertools.count()
BaseTrader.close_market_order = BaseTrader.open_market_order = lambda *kwargs: 'LOCAL{0}'.format(count.next())

def tearDown():
	Instrument.objects.filter(secid='XX1505').first().delete()
	a = Account.objects.filter(code='test').first()
	for o in a.orders:
		o.delete()
	a.delete()

def test_1():
	inst = Instrument.objects.get_or_create(secid='XX1505', name='XX1505', symbol='XX1505', quoted_currency='CNY', multiplier=1.0)
	trader = BaseTrader('test', 'CNY', 'XX1505:100')
	trader.set_monitors()
	thread = CheckStopThread(trader)
	order1 = trader.open_order(inst, 0.0, 1, True, 'anna1')
	trader.on_new_order(order1.local_id, 'XX1505', 'ORDER1', True, 0.0, 1, datetime.now())
	trader.on_trade('EXEC1', 'XX1505', 'ORDER1', 5000, 1, datetime.now())
	order1 = Order.objects.get_by_id(order1.id)
	eq_(order1.stopprice, 4900)

	thread.set_stopprice(inst, 5100, 100)
	order1 = Order.objects.get_by_id(order1.id)
	eq_(order1.stopprice, 5000)
	
	order2 = trader.open_order(inst, 0.0, 1, True, 'anna2')
	trader.on_new_order(order2.local_id, 'XX1505', 'ORDER2', True, 0.0, 1, datetime.now())
	trader.on_trade('EXEC2', 'XX1505', 'ORDER2', 4980, 1, datetime.now())
	order2 = Order.objects.get_by_id(order2.id)
	eq_(order2.stopprice, 4880)
	thread.set_stopprice(inst, 4880, 100)
	order1 = Order.objects.get_by_id(order1.id)
	eq_(order1.stopprice, 5000)

	thread.set_stopprice(inst, 5050, 100)
	order1 = Order.objects.get_by_id(order1.id)
	eq_(order1.stopprice, 5000)
	order2 = Order.objects.get_by_id(order2.id)
	eq_(order2.stopprice, 4950)
	
	thread.set_stopprice(inst, 5150, 100)
	order1 = Order.objects.get_by_id(order1.id)
	eq_(order1.stopprice, 5050)
	order2 = Order.objects.get_by_id(order2.id)
	eq_(order2.stopprice, 5050)
