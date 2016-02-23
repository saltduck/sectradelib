import datetime
from nose.tools import eq_

from ..models import Instrument

#for inst in Instrument.objects.filter(secid='XX1505'):
    #inst.delete()

def test_invalid_args():
    inst = Instrument()
    assert not inst.is_valid()
    eq_(inst.errors, [('quoted_currency', 'required'), ('secid', 'required'), ('name', 'required'), ('symbol', 'required')])
    inst = Instrument(secid='XX1505', name='XX 2015/05', symbol='XX-1505', quoted_currency='USD')
    assert inst.is_valid()

def test_classmethods():
    assert Instrument.from_id('XX1505') is None
    inst = Instrument.objects.create(secid='XX1505', name='XX 2015/05', symbol='XX-1505', quoted_currency='USD')
    eq_(Instrument.symbol2id('XX-1505'), 'XX1505')
    assert Instrument.symbol2id('XX1505') is None
    eq_(Instrument.from_id('XX1505'), inst)
    assert 'XX1505' in Instrument.all_ids()
    inst.delete()

def test_calcs():
    inst = Instrument(secid='XX1505', name='XX 2015/05', symbol='XX-1505', quoted_currency='USD', multiplier=100.0, long_margin_ratio=0.05, short_margin_ratio=0.1)
    eq_(inst.amount(100.0, 2), 20000.0)
    eq_(inst.calc_margin(100.0, 2, True), 1000.0)
    eq_(inst.calc_margin(100.0, 2, False), 2000.0)
    inst.open_commission_rate = 0.00005
    inst.close_commission_rate = 0.0
    eq_(inst.calc_commission(100.0, 2, True), 1.0)
    eq_(inst.calc_commission(100.0, 2, False), 0.0)
    inst.open_commission_rate = None
    inst.close_commission_rate = None
    eq_(inst.calc_commission(100.0, 2, True), 0.5)
    eq_(inst.calc_commission(100.0, 2, False), 0.5)

def test_deadline():
    inst = Instrument(secid='XX1505', name='XX 2015/05', symbol='XX-1505', quoted_currency='USD', expire_date=datetime.date(2015, 12, 31))
    inst.exchangeid=''
    eq_(inst.deadline().strftime('%Y-%m-%d %H:%M'), '2015-12-31 23:59')
    inst.exchangeid='CZCE'
    eq_(inst.deadline().strftime('%Y-%m-%d %H:%M'), '2015-11-30 14:00')
    inst.exchangeid='DCE'
    eq_(inst.deadline().strftime('%Y-%m-%d %H:%M'), '2015-11-30 14:00')
    inst.exchangeid='SHFE'
    eq_(inst.deadline().strftime('%Y-%m-%d %H:%M'), '2015-12-28 14:00')
    inst.exchangeid='CFFEX'
    eq_(inst.deadline().strftime('%Y-%m-%d %H:%M'), '2015-12-31 14:55')
