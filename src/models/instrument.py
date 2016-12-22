# coding:utf8
import logging
import datetime

from redisco import models

logger = logging.getLogger(__name__)


class Instrument(models.Model):
    """
    >>> i = Instrument(secid='XX1505', name='XX 2015/05', symbol='XX-1505', quoted_currency='USD', multiplier=10.0)
    >>> print i
    XX-1505
    """
    secid = models.Attribute(required=True)
    name = models.Attribute(required=True)
    symbol = models.Attribute(required=True)
    exchangeid = models.Attribute()
    product = models.ReferenceField('Product')
    quoted_currency = models.Attribute(required=True, indexed=False)
    indirect_quotation = models.BooleanField(indexed=False)
    ndigits = models.IntegerField(indexed=False, default=2)
    multiplier = models.FloatField(indexed=False)
    open_commission_rate = models.FloatField(indexed=False, default=0)
    close_commission_rate = models.FloatField(indexed=False, default=0)
    tick_size = models.FloatField(indexed=False)
    tick_value = models.FloatField(indexed=False)
    min_order_volume = models.FloatField(indexed=False, default=1.0)
    max_order_volume = models.FloatField(indexed=False, default=99999999.0)
    effective_date = models.DateField(indexed=False)
    expire_date = models.DateField(indexed=False)
    is_trading = models.BooleanField()
    long_margin_ratio = models.FloatField(indexed=False)
    short_margin_ratio = models.FloatField(indexed=False)
    volume = models.FloatField(indexed=False)

    def __repr__(self):
        return self.symbol

    def amount(self, price, volume):
        if not price:
            return 0.0
        if self.indirect_quotation:
            return volume * self.multiplier / price
        else:
            return volume * self.multiplier * price

    def calc_margin(self, price, volume, direction=None):
        if direction:
            return abs(self.amount(price, volume) * self.long_margin_ratio)
        else:
            return abs(self.amount(price, volume) * self.short_margin_ratio)

    def amount2volume(self, amt, price):
        if not price:
            return 0.0
        if self.indirect_quotation:
            return amt * price / self.multiplier
        else:
            return amt / price / self.multiplier

    def margin2volume(self, margin, price, direction=None):
        if direction:
            amt = margin / self.long_margin_ratio
        else:
            amt = margin / self.short_margin_ratio
        return self.amount2volume(amt, price)

    def calc_commission(self, price, volume, is_open):
        if is_open:
            if self.open_commission_rate is None:
                self.open_commission_rate = 0.000025
            commission = abs(self.amount(price, volume)) * self.open_commission_rate
        else:
            if self.close_commission_rate is None:
                self.close_commission_rate = 0.000025
            commission = abs(self.amount(price, volume)) * self.close_commission_rate
        ndigits = self.ndigits or 2
        return round(commission, ndigits)

    @classmethod
    def symbol2id(cls, symbol):
        instance = cls.objects.filter(symbol=symbol).first()
        if instance:
            return instance.secid

    @classmethod
    def from_id(cls, secid):
        return cls.objects.filter(secid=secid).first()

    @classmethod
    def all_ids(cls):
        return [obj.secid for obj in cls.objects.all()]

    def deadline(self):
        if self.exchangeid in ('DCE', 'CZCE'):    # 大连/郑州
            d = self.expire_date.replace(day=1) - datetime.date.resolution
            while d.weekday() >= 5: # weekend
                d = d - datetime.date.resolution
            t = datetime.time(14, 0)
        elif self.exchangeid == 'SHFE':  # 上期
            d = self.expire_date - datetime.timedelta(3)
            t = datetime.time(14, 0)
        elif self.exchangeid == 'CFFEX':  # 中金所
            d = self.expire_date
            t = datetime.time(14, 55)
        else:
            d = self.expire_date
            t = datetime.time(23, 59)
        return datetime.datetime.combine(d, t)


class Product(models.Model):
    """
    >>> p = Product.objects.create(prodid='XX', exchangeid='XXXX')
    >>> p
    XX
    >>> p.instruments
    []
    >>> i = Instrument.objects.create(secid='XX1505', name='XX1505', symbol='XX1505', product=p, quoted_currency='USD')
    >>> p.instruments
    [XX1505]
    >>> i.delete()
    >>> p.delete()
    """
    prodid = models.Attribute()
    exchangeid = models.Attribute()
    is_trading = models.BooleanField()
    main_inst = models.ReferenceField(Instrument)

    def __repr__(self):
        return self.prodid

    @property
    def instruments(self):
        return Instrument.objects.filter(product_id=self.id)
