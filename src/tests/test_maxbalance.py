
from nose.tools import eq_, with_setup

from sectradelib.trader import BaseTrader

def test_max_balance():
    trader = BaseTrader('test', 'test', 'CNY', '')
    eq_(trader.max_balance, 0.0)
    trader.account.set_balance(100.0)
    trader.on_account_changed()
    eq_(trader.max_balance, 100.0)
    trader.account.set_balance(200.0)
    trader.on_account_changed()
    eq_(trader.max_balance, 200.0)
    trader.account.set_balance(150.0)
    trader.on_account_changed()
    eq_(trader.max_balance, 200.0)
    trader.account.set_balance(50.0)
    trader.on_account_changed()
    eq_(trader.max_balance, 200.0)
    trader.account.set_balance(250.0)
    trader.on_account_changed()
    eq_(trader.max_balance, 250.0)
    trader.account.set_balance(200.0)
    trader.on_account_changed()
    eq_(trader.max_balance, 250.0)
    # test for day switch
    trader.on_day_switch()
    eq_(trader.max_balance, 0.0)
    trader.account.set_balance(100.0)
    trader.on_account_changed()
    eq_(trader.max_balance, 100.0)