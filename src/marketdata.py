# coding: utf8
import logging
from abc import ABCMeta, abstractmethod

import redisco

logger = logging.getLogger(__name__)
rdb = redisco.get_client()


class MarketDataApi(object):
    __metaclass__ = ABCMeta

    def __init__(self, quote_service, instruments, interval):
        self.quote_service = quote_service
        self.instruments = set(instruments)
        self.quote_service.interval = interval
        self.quote_service.mdapis.append(self)

    def start(self):
        self.subscribe(self.instruments)

    def process_tick(self, tick):
        # logger.debug(str(tick))
        secid = tick.securityID
        rdb.hset('current_price', secid, tick.price)
        if hasattr(tick, 'b_price'):
            rdb.hset('current_b_price', secid, tick.b_price)
        if hasattr(tick, 's_price'):
            rdb.hset('current_s_price', secid, tick.s_price)
        rdb.publish('checkstop', secid)
        with self.quote_service.tick_lock:
            self.quote_service.tickdata[secid].append(tick)

    @abstractmethod
    def subscribe(self, instruments):
        """ 订阅多个合约行情数据"""
