# coding: utf8
import logging
import os.path
import threading
import Queue
from time import sleep
from collections import defaultdict
from operator import attrgetter
from datetime import datetime
from decimal import Decimal

import pandas as pd
import redisco

from utils import current_price # for backward compatible

logger = logging.getLogger(__name__)
rdb = redisco.get_client()


class TickObject(dict):
    """ 代表一条行情数据 """
    def __getattr__(self, attr):
        if attr in self:
            return self[attr]


class QuoteService(threading.Thread):
    def __init__(self):
        super(QuoteService, self).__init__(name='QUOTESERVICE')
        self.tickdata = defaultdict(list)
        self.market_closed = defaultdict(bool)
        self.last_save_time = {}
        self.interval = 10
        self.mdapis = []

        self.is_running = True
        self.tick_lock = threading.RLock()
        self.evt_newmindata = threading.Event()

    @property
    def instruments(self):
        return self.tickdata.keys()

    def subscribe(self, seclist):
        for api in self.mdapis:
            api.subscribe([inst.secid for inst in seclist])

    def unsubscribe(self, seclist):
        for api in self.mdapis:
            api.unsubscribe([inst.secid for inst in seclist])

    def wait_for_subscribe(self):
        from .models import Instrument
        ps = rdb.pubsub()
        ps.subscribe('mdmonitor')
        while self.is_running:
            for item in ps.listen():
                if item['type'] == 'message':
                    inst = Instrument.objects.filter(secid=item['data']).first()
                    logger.debug(u'Subscribe market data for:{0}'.format(inst))
                    product = inst.product
                    if product:
                        self.unsubscribe(product.instruments)
                    self.subscribe([inst])
            sleep(1)

    def stop(self):
        self.is_running = False

    def run(self):
        logger.info('Quote Service is starting...')
        threading.Thread(target=self.wait_for_subscribe).start()
        while self.is_running:
            sleep(0.2)
            saved = False
            for instid in self.instruments:
                try:
                    saved = self.save_inst_mindata(instid) or saved
                except Exception, e:
                    logger.exception(unicode(e))
                    pass
            if saved:
                self.evt_newmindata.set()
                sleep(0.1)
                self.evt_newmindata.clear()
        logger.info('Quote Service exited...')

    def do_save(self, df):
        pass

    def save_inst_mindata(self, inst):
        ticks = self.tickdata[inst]
        if not ticks:
            self.market_closed[inst] = False
            return False
        if not self.market_closed[inst] and (ticks[-1].entry_time - ticks[0].entry_time).seconds < self.interval:
            # logger.debug('No enough data')
            return False
        logger.info(u'保存合约{0}的分钟数据...'.format(inst))
        df = pd.DataFrame.from_records(ticks, index='entry_time')
        rule = '{0}s'.format(self.interval)
        df2 = df.resample(rule, label='right', how={'price': 'ohlc'}).price
        df2['volume'] = df.resample(rule, label='right', how={'volume': 'sum'})
        df3 = df2.dropna(axis=0)
        logger.debug(df3)
        if not self.market_closed[inst]:
            df3 = df3[:-1]
        self.market_closed[inst] = False
        if df3.empty:
            return False
        df3['securityID'] = inst
        df3 = df3.rename(columns={
            'open': 'open_price',
            'close': 'close_price',
            'high': 'high_price',
            'low': 'low_price'})
        self.do_save(df3)
        self.last_save_time[inst] = df3.ix[-1].name
        with self.tick_lock:
            self.tickdata[inst] = [tick for tick in self.tickdata[inst] if tick.entry_time >= self.last_save_time[inst]]
            self.tickdata[inst].sort(key=attrgetter('entry_time'))
        logger.debug('Saved @ {0}'.format(self.last_save_time[inst]))
        return True
