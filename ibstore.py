#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015-2020 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import collections
from copy import copy
from datetime import date, datetime, timedelta
import inspect
import itertools
import random
import threading
import time
import bisect
import asyncio

import logging

#from ib.ext.Contract import Contract
#import ib.opt as ibopt

from ib_insync import Contract, IB, util
#import cerebro

from backtrader import TimeFrame, Position
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import queue, with_metaclass, long
from backtrader.utils import AutoDict, UTC


def _ts2dt(tstamp=None):
    # Transforms a RTVolume timestamp to a datetime object
    if not tstamp:
        return datetime.utcnow()

    sec, msec = divmod(long(tstamp), 1000)
    usec = msec * 1000
    return datetime.utcfromtimestamp(sec).replace(microsecond=usec)


class RTVolume(object):
    '''Parses a tickString tickType 48 (RTVolume) event from the IB API into its
    constituent fields

    Supports using a "price" to simulate an RTVolume from a tickPrice event
    '''
    _fields = [
        ('price', float),
        ('size', int),
        ('datetime', _ts2dt),
        ('volume', int),
        ('vwap', float),
        ('single', bool)
    ]

    def __init__(self, rtvol='', price=None, tmoffset=None):
        # Use a provided string or simulate a list of empty tokens
        tokens = iter(rtvol.split(';'))

        # Put the tokens as attributes using the corresponding func
        for name, func in self._fields:
            setattr(self, name, func(next(tokens)) if rtvol else func())

        # If price was provided use it
        if price is not None:
            self.price = price

        if tmoffset is not None:
            self.datetime += tmoffset

class MktData:

    async def update_ticks(self, ib, contract, ticks, q_ticks):
            ib.reqMktData(contract, ticks) # ticks=233 last, lastSize, rtVolume, rtTime, vwap (Time & Sales)
                #self.ib.reqRealTimeBars(contract, 5, 'MIDPOINT', 
                #                    useRTH=False)
                #ib.reqTickByTickData(contract, 'MidPoint')

            async for tickers in ib.pendingTickersEvent:
                for ticker in tickers:
                    q_ticks.put(ticker)
                return

class MetaSingleton(MetaParams):
    '''Metaclass to make a metaclassed class a singleton'''
    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super(MetaSingleton, cls).__call__(*args, **kwargs))

        return cls._singleton


class IBStore(with_metaclass(MetaSingleton, object)):
    '''Singleton class wrapping an ibpy ibConnection instance.

    The parameters can also be specified in the classes which use this store,
    like ``IBData`` and ``IBBroker``

    Params:

      - ``host`` (default:``127.0.0.1``): where IB TWS or IB Gateway are
        actually running. And although this will usually be the localhost, it
        must not be

      - ``port`` (default: ``7496``): port to connect to. The demo system uses
        ``7497``

      - ``clientId`` (default: ``None``): which clientId to use to connect to
        TWS.

        ``None``: generates a random id between 1 and 65535
        An ``integer``: will be passed as the value to use.

      - ``notifyall`` (default: ``False``)

        If ``False`` only ``error`` messages will be sent to the
        ``notify_store`` methods of ``Cerebro`` and ``Strategy``.

        If ``True``, each and every message received from TWS will be notified

      - ``_debug`` (default: ``False``)

        Print all messages received from TWS to standard output

      - ``reconnect`` (default: ``3``)

        Number of attempts to try to reconnect after the 1st connection attempt
        fails

        Set it to a ``-1`` value to keep on reconnecting forever

      - ``timeout`` (default: ``3.0``)

        Time in seconds between reconnection attemps

      - ``timeoffset`` (default: ``True``)

        If True, the time obtained from ``reqCurrentTime`` (IB Server time)
        will be used to calculate the offset to localtime and this offset will
        be used for the price notifications (tickPrice events, for example for
        CASH markets) to modify the locally calculated timestamp.

        The time offset will propagate to other parts of the ``backtrader``
        ecosystem like the **resampling** to align resampling timestamps using
        the calculated offset.

      - ``timerefresh`` (default: ``60.0``)

        Time in seconds: how often the time offset has to be refreshed

      - ``indcash`` (default: ``True``)

        Manage IND codes as if they were cash for price retrieval
    '''

    # Set a base for the data requests (historical/realtime) to distinguish the
    # id in the error notifications from orders, where the basis (usually
    # starting at 1) is set by TWS
    REQIDBASE = 0x01000000

    BrokerCls = None #getattr(sys.modules["cerebro.strategies." +classname.split('.')[0]], classname.split('.')[1])IBBroker  #None  # broker class will autoregister
    DataCls = None  # data class will auto register

    params = (
        ('host', '127.0.0.1'),
        ('port', 7496),
        ('clientId', None),  # None generates a random clientid 1 -> 2^16
        ('notifyall', False), # NOT IMPLEMENTED
        ('_debug', False),
        ('reconnect', 3),  # -1 forever, 0 No, > 0 number of retries
        ('timeout', 3.0),  # timeout between reconnections
        ('timeoffset', True),  # Use offset to server for timestamps if needed
        ('timerefresh', 60.0),  # How often to refresh the timeoffset
        ('indcash', True),  # Treat IND codes as CASH elements
        ('readonly', False),  # Set to True when IB API is in read-only mode
        ('account', ''),  # Main account to receive updates for
     
    )

    @classmethod
    def getdata(cls, *args, **kwargs):
        '''Returns ``DataCls`` with args, kwargs'''
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        '''Returns broker with *args, **kwargs from registered ``BrokerCls``'''
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        super(IBStore, self).__init__()

        self._env = None  # reference to cerebro for general notifications
        self.broker = None  # broker instance
        self.datas = list()  # datas that have registered over start
        # self.ccount = 0  # requests to start (from cerebro or datas)

        # self._lock_tmoffset = threading.Lock()
        # self.tmoffset = timedelta()  # to control time difference with server

        # # Structures to hold datas requests
        # self.qs = collections.OrderedDict()  # key: tickerId -> queues
        # self.ts = collections.OrderedDict()  # key: queue -> tickerId
        self.iscash = dict()  # tickerIds from cash products (for ex: EUR.JPY)

        # self.histexreq = dict()  # holds segmented historical requests
        # self.histfmt = dict()  # holds datetimeformat for request
        # self.histsend = dict()  # holds sessionend (data time) for request
        # self.histtz = dict()  # holds sessionend (data time) for request

        self.acc_cash = AutoDict()  # current total cash per account
        self.acc_value = AutoDict()  # current total value per account
        self.acc_upds = AutoDict()  # current account valueinfos per account

        # self.port_update = False  # indicate whether to signal to broker

        self.positions = collections.defaultdict(Position)  # actual positions

        #self._tickerId = itertools.count(self.REQIDBASE)  # unique tickerIds
        self.orderid = None  # next possible orderid (will be itertools.count)

        # self.cdetails = collections.defaultdict(list)  # hold cdetails requests

        self.managed_accounts = list()  # received via managedAccounts

        self.notifs = queue.Queue()  # store notifications for cerebro
        
        self.orders = collections.OrderedDict()  # orders by order ided

        self.opending = collections.defaultdict(list)  # pending transmission
        self.brackets = dict()  # confirmed brackets
        
        self.last_tick = None

        # Use the provided clientId or a random one
        if self.p.clientId is None:
            self.clientId = random.randint(1, pow(2, 16) - 1)
        else:
            self.clientId = self.p.clientId

        if self.p.timeout is None:
            self.timeout = 2
        else:
            self.timeout = self.p.timeout

        if self.p.readonly is None:
            self.readonly = False
        else:
            self.readonly = self.p.readonly

        if self.p.account is None:
            self.account = ""
        else:
            self.account = self.p.account
                    
        if self.p._debug:
            util.logToConsole(level=logging.DEBUG)
        
        util.patchAsyncio()
        util.startLoop()
        
        self.ib = IB()
        
        self.ib.connect(
                    host=self.p.host, port=self.p.port, 
                    clientId=self.clientId,
                    timeout=self.timeout,
                    readonly=self.readonly,
                    account=self.account,
                    )
        
        # # register a printall method if requested
        # if self.p._debug or self.p.notifyall:
        #     self.conn.registerAll(self.watcher)

        # Register decorated methods with the conn
        # methods = inspect.getmembers(self, inspect.ismethod)
        # for name, method in methods:
        #     if not getattr(method, '_ibregister', False):
        #         continue

        #     message = getattr(ibopt.message, name)
        #     self.conn.register(method, message)

        # This utility key function transforms a barsize into a:
        #   (Timeframe, Compression) tuple which can be sorted
        def keyfn(x):
            n, t = x.split()
            tf, comp = self._sizes[t]
            return (tf, int(n) * comp)

        # This utility key function transforms a duration into a:
        #   (Timeframe, Compression) tuple which can be sorted
        def key2fn(x):
            n, d = x.split()
            tf = self._dur2tf[d]
            return (tf, int(n))

        # Generate a table of reverse durations
        self.revdur = collections.defaultdict(list)
        # The table (dict) is a ONE to MANY relation of
        #   duration -> barsizes
        # Here it is reversed to get a ONE to MANY relation of
        #   barsize -> durations
        for duration, barsizes in self._durations.items():
            for barsize in barsizes:
                self.revdur[keyfn(barsize)].append(duration)

        # Once managed, sort the durations according to real duration and not
        # to the text form using the utility key above
        for barsize in self.revdur:
            self.revdur[barsize].sort(key=key2fn)

    def start(self, data=None, broker=None):
        #self.reconnect(fromstart=True)  # reconnect should be an invariant

        # Datas require some processing to kickstart data reception
        if data is not None:
            self._env = data._env
            # For datas simulate a queue with None to kickstart co
            self.datas.append(data)

            # if connection fails, get a fakeation that will force the
            # datas to try to reconnect or else bail out
            return self.getTickerQueue(start=True)

        elif broker is not None:
            self.broker = broker

    def stop(self):
        try:
            self.ib.disconnect()  # disconnect should be an invariant
        except AttributeError:
            pass    # conn may have never been connected and lack "disconnect"

    def get_notifications(self):
        '''Return the pending "store" notifications'''
        # The background thread could keep on adding notifications. The None
        # mark allows to identify which is the last notification to deliver
        self.notifs.put(None)  # put a mark
        notifs = list()
        while True:
            notif = self.notifs.get()
            if notif is None:  # mark is reached
                break
            notifs.append(notif)

        return notifs

    def managedAccounts(self):
        # 1st message in the stream
        ma = self.ib.managedAccounts()
        self.managed_accounts = ma
        # self._event_managed_accounts.set()

        # Request time to avoid synchronization issues
        self.reqCurrentTime()

    def currentTime(self,msg):
        if not self.p.timeoffset:  # only if requested ... apply timeoffset
            return
        curtime = datetime.fromtimestamp(float(msg.time))
        with self._lock_tmoffset:
            self.tmoffset = curtime - datetime.now()

        threading.Timer(self.p.timerefresh, self.reqCurrentTime).start()

    def timeoffset(self):
        with self._lock_tmoffset:
            return self.tmoffset

    def reqCurrentTime(self):
        self.ib.reqCurrentTime()

    # def nextTickerId(self):
    #     # Get the next ticker using next on the itertools.count
    #     return next(self._tickerId)

    def nextOrderId(self):
        # Get the next ticker using a new request value from TWS
        self.orderid = self.ib.client.getReqId()
        return self.orderid

    # def reuseQueue(self, tickerId):
    #     '''Reuses queue for tickerId, returning the new tickerId and q'''
    #     with self._lock_q:
    #         # Invalidate tickerId in qs (where it is a key)
    #         q = self.qs.pop(tickerId, None)  # invalidate old
    #         iscash = self.iscash.pop(tickerId, None)

    #         # Update ts: q -> ticker
    #         tickerId = self.nextTickerId()  # get new tickerId
    #         self.ts[q] = tickerId  # Update ts: q -> tickerId
    #         self.qs[tickerId] = q  # Update qs: tickerId -> q
    #         self.iscash[tickerId] = iscash

    #     return tickerId, q

    def getTickerQueue(self, start=False):
        '''Creates ticker/Queue for data delivery to a data feed'''
        q = queue.Queue()
        if start:
            q.put(None)
            return q
            
        #tickerId = self.nextTickerId()
        # self.qs[tickerId] = q  # can be managed from other thread
        # self.ts[q] = tickerId
        # self.iscash[tickerId] = False

        #return tickerId, q
        return q
    

    # def cancelQueue(self, q, sendnone=False):
    #     '''Cancels a Queue for data delivery'''
    #     # pop ts (tickers) and with the result qs (queues)
    #     tickerId = self.ts.pop(q, None)
    #     self.qs.pop(tickerId, None)

    #     self.iscash.pop(tickerId, None)

    #     if sendnone:
    #         q.put(None)

    # def validQueue(self, q):
    #     '''Returns (bool)  if a queue is still valid'''
    #     return q in self.ts  # queue -> ticker

    def getContractDetails(self, contract, maxcount=None):
        #cds = list()
        cds = self.ib.reqContractDetails(contract)
 
        #cds.append(cd)

        if not cds or (maxcount and len(cds) > maxcount):
            err = 'Ambiguous contract: none/multiple answers received'
            self.notifs.put((err, cds, {}))
            return None

        return cds

    def reqHistoricalDataEx(self, contract, enddate, begindate,
                            timeframe, compression,
                            what=None, useRTH=False, tz='', 
                            sessionend=None,
                            #tickerId=None
                            ):
        '''
        Extension of the raw reqHistoricalData proxy, which takes two dates
        rather than a duration, barsize and date

        It uses the IB published valid duration/barsizes to make a mapping and
        spread a historical request over several historical requests if needed
        '''
        # Keep a copy for error reporting purposes
        kwargs = locals().copy()
        kwargs.pop('self', None)  # remove self, no need to report it

        if timeframe < TimeFrame.Seconds:
            # Ticks are not supported
            return self.getTickerQueue(start=True)

        if enddate is None:
            enddate = datetime.now()

        if begindate is None:
            duration = self.getmaxduration(timeframe, compression)
            if duration is None:
                err = ('No duration for historical data request for '
                       'timeframe/compresison')
                self.notifs.put((err, (), kwargs))
                return self.getTickerQueue(start=True)
            barsize = self.tfcomp_to_size(timeframe, compression)
            if barsize is None:
                err = ('No supported barsize for historical data request for '
                       'timeframe/compresison')
                self.notifs.put((err, (), kwargs))
                return self.getTickerQueue(start=True)

            return self.reqHistoricalData(contract=contract, enddate=enddate,
                                          duration=duration, barsize=barsize,
                                          what=what, useRTH=useRTH, tz=tz,
                                          sessionend=sessionend)

        # Check if the requested timeframe/compression is supported by IB
        durations = self.getdurations(timeframe, compression)
        # if not durations:  # return a queue and put a None in it
        #     return self.getTickerQueue(start=True)

        # Get or reuse a queue
        # if tickerId is None:
        #     tickerId, q = self.getTickerQueue()
        # else:
        #     tickerId, q = self.reuseQueue(tickerId)  # reuse q for old tickerId

        # Get the best possible duration to reduce number of requests
        duration = None
        # for dur in durations:
        #     intdate = self.dt_plus_duration(begindate, dur)
        #     if intdate >= enddate:
        #         intdate = enddate
        #         duration = dur  # begin -> end fits in single request
        #         break
        intdate = begindate

        if duration is None:  # no duration large enough to fit the request
            duration = durations[-1]

            # Store the calculated data
            # self.histexreq[tickerId] = dict(
            #     contract=contract, enddate=enddate, begindate=intdate,
            #     timeframe=timeframe, compression=compression,
            #     what=what, useRTH=useRTH, tz=tz, sessionend=sessionend)

        barsize = self.tfcomp_to_size(timeframe, compression)
        # self.histfmt[tickerId] = timeframe >= TimeFrame.Days
        # self.histsend[tickerId] = sessionend
        # self.histtz[tickerId] = tz

        if contract.secType in ['CASH', 'CFD']:
            #self.iscash[tickerId] = 1  # msg.field code
            if not what:
                what = 'BID'  # default for cash unless otherwise specified

        elif contract.secType in ['IND'] and self.p.indcash:
            #self.iscash[tickerId] = 4  # msg.field code
            pass

        what = what or 'TRADES'
        
        q = self.getTickerQueue()

        histdata = self.ib.reqHistoricalData(
                                contract,
                                intdate.strftime('%Y%m%d %H:%M:%S') + ' GMT',
                                duration,
                                barsize,
                                what,
                                useRTH,
                                2) # dateformat 1 for string, 2 for unix time in seconds
        for msg in histdata:
            q.put(msg)
        
        return q

    def reqHistoricalData(self, contract, enddate, duration, barsize,
                          what=None, useRTH=False, tz='', sessionend=None):
        '''Proxy to reqHistorical Data'''

        # get a ticker/queue for identification/data delivery
        q = self.getTickerQueue()

        if contract.secType in ['CASH', 'CFD']:
            #self.iscash[tickerId] = True
            if not what:
                what = 'BID'  # TRADES doesn't work
            elif what == 'ASK':
                #self.iscash[tickerId] = 2
                pass
        else:
            what = what or 'TRADES'

        # split barsize "x time", look in sizes for (tf, comp) get tf
        #tframe = self._sizes[barsize.split()[1]][0]
        # self.histfmt[tickerId] = tframe >= TimeFrame.Days
        # self.histsend[tickerId] = sessionend
        # self.histtz[tickerId] = tz

        histdata = self.ib.reqHistoricalData(
                            contract,
                            enddate.strftime('%Y%m%d %H:%M:%S') + ' GMT',
                            duration,
                            barsize,
                            what,
                            useRTH,
                            2) # dateformat 1 for string, 2 for unix time in seconds
        for msg in histdata:
            q.put(msg)

        return q

    # def cancelHistoricalData(self, q):
    #     '''Cancels an existing HistoricalData request

    #     Params:
    #       - q: the Queue returned by reqMktData
    #     '''
    #     with self._lock_q:
    #         self.ib.cancelHistoricalData(self.ts[q])
    #         self.cancelQueue(q, True)

    def reqRealTimeBars(self, contract, useRTH=False, duration=5):
        '''Creates a request for (5 seconds) Real Time Bars

        Params:
          - contract: a ib.ext.Contract.Contract intance
          - useRTH: (default: False) passed to TWS
          - duration: (default: 5) passed to TWS

        Returns:
          - a Queue the client can wait on to receive a RTVolume instance
        '''
        # get a ticker/queue for identification/data delivery
        q = self.getTickerQueue()

        rtb = self.ib.reqRealTimeBars(contract, duration, 
                                      'MIDPOINT', useRTH=useRTH)
        self.ib.sleep(duration)
        for bar in rtb:
            q.put(bar)
        return q

    def reqMktData(self, contract, what=None):
        '''Creates a MarketData subscription

        Params:
          - contract: a ib.ext.Contract.Contract intance

        Returns:
          - a Queue the client can wait on to receive a RTVolume instance
        '''
        # get a ticker/queue for identification/data delivery
        q = self.getTickerQueue()
        ticks = '233'  # request RTVOLUME tick delivered over tickString

        if contract.secType in ['CASH', 'CFD']:
            #self.iscash[tickerId] = True
            ticks = ''  # cash markets do not get RTVOLUME
            if what == 'ASK':
                #self.iscash[tickerId] = 2
                pass

        # q.put(None)  # to kickstart backfilling
        # Can request 233 also for cash ... nothing will arrive
        
        md = MktData()
        q_ticks = queue.Queue()
        
        util.run(md.update_ticks(self.ib, contract, ticks, q_ticks))
                  
        while not q_ticks.empty():
            ticker = q_ticks.get()
            for tick in ticker.ticks:
                # https://interactivebrokers.github.io/tws-api/tick_types.html
                if tick != self.last_tick: #last price
                    #print(str(tick.time) +" >> " + str(tick.price))
                    self.last_tick = tick
                    q.put(tick)

        return q

    # def cancelMktData(self, q):
    #     '''Cancels an existing MarketData subscription

    #     Params:
    #       - q: the Queue returned by reqMktData
    #     '''
    #     with self._lock_q:
    #         tickerId = self.ts.get(q, None)
    #         if tickerId is not None:
    #             self.ib.cancelMktData(tickerId)

    #         self.cancelQueue(q, True)


    # The _durations are meant to calculate the needed historical data to
    # perform backfilling at the start of a connetion or a connection is lost.
    # Using a timedelta as a key allows to quickly find out which bar size
    # bar size (values in the tuples int the dict) can be used.

    _durations = dict([
        # 60 seconds - 1 min
        ('60 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min')),

        # 120 seconds - 2 mins
        ('120 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins')),

        # 180 seconds - 3 mins
        ('180 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins')),

        # 300 seconds - 5 mins
        ('300 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins')),

        # 600 seconds - 10 mins
        ('600 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins')),

        # 900 seconds - 15 mins
        ('900 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins')),

        # 1200 seconds - 20 mins
        ('1200 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins')),

        # 1800 seconds - 30 mins
        ('1800 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins')),

        # 3600 seconds - 1 hour
        ('3600 S',
         ('5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour')),

        # 7200 seconds - 2 hours
        ('7200 S',
         ('5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour', '2 hours')),

        # 10800 seconds - 3 hours
        ('10800 S',
         ('10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour', '2 hours', '3 hours')),

        # 14400 seconds - 4 hours
        ('14400 S',
         ('15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour', '2 hours', '3 hours', '4 hours')),

        # 28800 seconds - 8 hours
        ('28800 S',
         ('30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour', '2 hours', '3 hours', '4 hours', '8 hours')),

        # 1 days
        ('1 D',
         ('1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour', '2 hours', '3 hours', '4 hours', '8 hours',
          '1 day')),

        # 2 days
        ('2 D',
         ('2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour', '2 hours', '3 hours', '4 hours', '8 hours',
          '1 day')),

        # 1 weeks
        ('1 W',
         ('3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour', '2 hours', '3 hours', '4 hours', '8 hours',
          '1 day', '1 W')),

        # 2 weeks
        ('2 W',
         ('15 mins', '20 mins', '30 mins',
          '1 hour', '2 hours', '3 hours', '4 hours', '8 hours',
          '1 day', '1 W')),

        # 1 months
        ('1 M',
         ('30 mins',
          '1 hour', '2 hours', '3 hours', '4 hours', '8 hours',
          '1 day', '1 W', '1 M')),

        # 2+ months
        ('2 M', ('1 day', '1 W', '1 M')),
        ('3 M', ('1 day', '1 W', '1 M')),
        ('4 M', ('1 day', '1 W', '1 M')),
        ('5 M', ('1 day', '1 W', '1 M')),
        ('6 M', ('1 day', '1 W', '1 M')),
        ('7 M', ('1 day', '1 W', '1 M')),
        ('8 M', ('1 day', '1 W', '1 M')),
        ('9 M', ('1 day', '1 W', '1 M')),
        ('10 M', ('1 day', '1 W', '1 M')),
        ('11 M', ('1 day', '1 W', '1 M')),

        # 1+ years
        ('1 Y',  ('1 day', '1 W', '1 M')),
    ])

    # Sizes allow for quick translation from bar sizes above to actual
    # timeframes to make a comparison with the actual data
    _sizes = {
        'secs': (TimeFrame.Seconds, 1),
        'min': (TimeFrame.Minutes, 1),
        'mins': (TimeFrame.Minutes, 1),
        'hour': (TimeFrame.Minutes, 60),
        'hours': (TimeFrame.Minutes, 60),
        'day': (TimeFrame.Days, 1),
        'W': (TimeFrame.Weeks, 1),
        'M': (TimeFrame.Months, 1),
    }

    _dur2tf = {
        'S': TimeFrame.Seconds,
        'D': TimeFrame.Days,
        'W': TimeFrame.Weeks,
        'M': TimeFrame.Months,
        'Y': TimeFrame.Years,
    }

    def getdurations(self,  timeframe, compression):
        key = (timeframe, compression)
        if key not in self.revdur:
            return []

        return self.revdur[key]

    def getmaxduration(self, timeframe, compression):
        key = (timeframe, compression)
        try:
            return self.revdur[key][-1]
        except (KeyError, IndexError):
            pass

        return None

    def tfcomp_to_size(self, timeframe, compression):
        if timeframe == TimeFrame.Months:
            return '{} M'.format(compression)

        if timeframe == TimeFrame.Weeks:
            return '{} W'.format(compression)

        if timeframe == TimeFrame.Days:
            if not compression % 7:
                return '{} W'.format(compression // 7)

            return '{} day'.format(compression)

        if timeframe == TimeFrame.Minutes:
            if not compression % 60:
                hours = compression // 60
                return ('{} hour'.format(hours)) + ('s' * (hours > 1))

            return ('{} min'.format(compression)) + ('s' * (compression > 1))

        if timeframe == TimeFrame.Seconds:
            return '{} secs'.format(compression)

        # Microseconds or ticks
        return None

    def dt_plus_duration(self, dt, duration):
        size, dim = duration.split()
        size = int(size)
        if dim == 'S':
            return dt + timedelta(seconds=size)

    #     if dim == 'D':
    #         return dt + timedelta(days=size)

    #     if dim == 'W':
    #         return dt + timedelta(days=size * 7)

    #     if dim == 'M':
    #         month = dt.month - 1 + size  # -1 to make it 0 based, readd below
    #         years, month = divmod(month, 12)
    #         return dt.replace(year=dt.year + years, month=month + 1)

    #     if dim == 'Y':
    #         return dt.replace(year=dt.year + size)

    #     return dt  # could do nothing with it ... return it intact


    # def histduration(self, dt1, dt2):
    #     # Given two dates calculates the smallest possible duration according
    #     # to the table from the Historical Data API limitations provided by IB
    #     #
    #     # Seconds: 'x S' (x: [60, 120, 180, 300, 600, 900, 1200, 1800, 3600,
    #     #                     7200, 10800, 14400, 28800])
    #     # Days: 'x D' (x: [1, 2]
    #     # Weeks: 'x W' (x: [1, 2])
    #     # Months: 'x M' (x: [1, 11])
    #     # Years: 'x Y' (x: [1])

    #     td = dt2 - dt1  # get a timedelta for calculations

    #     # First: array of secs
    #     tsecs = td.total_seconds()
    #     secs = [60, 120, 180, 300, 600, 900, 1200, 1800, 3600, 7200, 10800,
    #             14400, 28800]

    #     idxsec = bisect.bisect_left(secs, tsecs)
    #     if idxsec < len(secs):
    #         return '{} S'.format(secs[idxsec])

    #     tdextra = bool(td.seconds or td.microseconds)  # over days/weeks

    #     # Next: 1 or 2 days
    #     days = td.days + tdextra
    #     if td.days <= 2:
    #         return '{} D'.format(days)

    #     # Next: 1 or 2 weeks
    #     weeks, d = divmod(td.days, 7)
    #     weeks += bool(d or tdextra)
    #     if weeks <= 2:
    #         return '{} W'.format(weeks)

    #     # Get references to dt components
    #     y2, m2, d2 = dt2.year, dt2.month, dt2.day
    #     y1, m1, d1 = dt1.year, dt1.month, dt2.day

    #     H2, M2, S2, US2 = dt2.hour, dt2.minute, dt2.second, dt2.microsecond
    #     H1, M1, S1, US1 = dt1.hour, dt1.minute, dt1.second, dt1.microsecond

    #     # Next: 1 -> 11 months (11 incl)
    #     months = (y2 * 12 + m2) - (y1 * 12 + m1) + (
    #         (d2, H2, M2, S2, US2) > (d1, H1, M1, S1, US1))
    #     if months <= 1:  # months <= 11
    #         return '1 M'  # return '{} M'.format(months)
    #     elif months <= 11:
    #         return '2 M'  # cap at 2 months to keep the table clean

    #     # Next: years
    #     # y = y2 - y1 + (m2, d2, H2, M2, S2, US2) > (m1, d1, H1, M1, S1, US1)
    #     # return '{} Y'.format(y)

    #     return '1 Y'  # to keep the table clean

    def makecontract(self, symbol, sectype, exch, curr,
                     expiry='', strike=0.0, right='', mult=1):
        '''returns a contract from the parameters without check'''

        contract = Contract()

        contract.symbol = symbol
        contract.secType = sectype
        contract.exchange = exch
        if curr:
            contract.currency = curr
        if sectype in ['FUT', 'OPT', 'FOP']:
            contract.expiry = expiry
        if sectype in ['OPT', 'FOP']:
            contract.strike = strike
            contract.right = right
        if mult:
            contract.multiplier = mult

        return contract

    def cancelOrder(self, orderid):
        '''Proxy to cancelOrder'''
        self.ib.cancelOrder(orderid)


    def placeOrder(self, orderid, contract, order):
        '''Proxy to placeOrder'''        
        trade = self.ib.placeOrder(contract, order)  
        while not trade.isDone():
            self.ib.waitOnUpdate()
        return trade
        
    # #@ibregister
    # def openOrder(self, msg):
    #     '''Receive the event ``openOrder`` events'''
    #     self.broker.push_orderstate(msg)

    #@ibregister
    # def execDetails(self, msg):
    #     '''Receive execDetails'''
    #     self.broker.push_execution(msg.execution)

    #@ibregister
        
    #def orderStatus(self):
        
        # orders = self.ib.openOrders()
        # '''Receive the event ``orderStatus``'''
        # if len(orders) > 1:
        #              print(orders)
        #self.push_orderstatus(msg)
        


    # #@ibregister
    # def commissionReport(self, msg):
    #     '''Receive the event commissionReport'''
    #     self.broker.push_commissionreport(msg.commissionReport)

    def reqTrades(self):
        '''Proxy to Trades'''
        return self.ib.trades()

    def reqPositions(self):
        '''Proxy to reqPositions'''
        return self.ib.reqPositions()
    
    def getposition(self, contract, clone=False):
        # Lock access to the position dicts. This is called from main thread
        # and updates could be happening in the background
        #with self._lock_pos:
        position = self.positions[contract.conId]
        if clone:
            return copy(position)

        return position

    def reqAccountUpdates(self, subscribe=True, account=None):
        '''Proxy to reqAccountUpdates

        If ``account`` is ``None``, wait for the ``managedAccounts`` message to
        set the account codes
        '''
        if account is None:
            #self._event_managed_accounts.wait()
            self.managedAccounts()
            account = self.managed_accounts[0]

        #self.ib.reqAccountUpdates(subscribe, bytes(account))
        self.updateAccountValue()

    # def accountDownloadEnd(self):
    #     # Signals the end of an account update
    #     # the event indicates it's over. It's only false once, and can be used
    #     # to find out if it has at least been downloaded once
    #     self._event_accdownload.set()
    #     if False:
    #         if self.port_update:
    #             self.broker.push_portupdate()

    #             self.port_update = False


    def updateAccountValue(self):
        # Lock access to the dicts where values are updated. This happens in a
        # sub-thread and could kick it at anytime
        #with self._lock_accupd:
        #if self.connected():
        ret = self.ib.accountValues()
        
        for msg in ret:
            try:
                value = float(msg.value)   
            except ValueError:
                value = msg.value

            self.acc_upds[msg.account][msg.tag][msg.currency] = value

            if msg.tag == 'NetLiquidation':
                # NetLiquidationByCurrency and currency == 'BASE' is the same
                self.acc_value[msg.account] = value
            elif msg.tag == 'TotalCashBalance' and msg.currency == 'BASE':
                self.acc_cash[msg.account] = value

    def get_acc_values(self, account=None):
        '''Returns all account value infos sent by TWS during regular updates
        Waits for at least 1 successful download

        If ``account`` is ``None`` then a dictionary with accounts as keys will
        be returned containing all accounts

        If account is specified or the system has only 1 account the dictionary
        corresponding to that account is returned
        '''
        # Wait for at least 1 account update download to have been finished
        # before the account infos can be returned to the calling client
        # if self.connected():
        #     self._event_accdownload.wait()
        # Lock access to acc_cash to avoid an event intefering
        #with self._updacclock:
        if account is None:
            # wait for the managedAccount Messages
            # if self.connected():
            #     self._event_managed_accounts.wait()

            if not self.managed_accounts:
                return self.acc_upds.copy()

            elif len(self.managed_accounts) > 1:
                return self.acc_upds.copy()

            # Only 1 account, fall through to return only 1
            account = self.managed_accounts[0]

        try:
            return self.acc_upds[account].copy()
        except KeyError:
            pass

        return self.acc_upds.copy()

    def get_acc_value(self, account=None):
        '''Returns the net liquidation value sent by TWS during regular updates
        Waits for at least 1 successful download

        If ``account`` is ``None`` then a dictionary with accounts as keys will
        be returned containing all accounts

        If account is specified or the system has only 1 account the dictionary
        corresponding to that account is returned
        '''
        # Wait for at least 1 account update download to have been finished
        # before the value can be returned to the calling client
        # if self.connected():
        #     self._event_accdownload.wait()
        # Lock access to acc_cash to avoid an event intefering
        #with self._lock_accupd:
        if account is None:
            # wait for the managedAccount Messages
            # if self.connected():
            #     self._event_managed_accounts.wait()

            if not self.managed_accounts:
                return float()

            elif len(self.managed_accounts) > 1:
                return sum(self.acc_value.values())

            # Only 1 account, fall through to return only 1
            account = self.managed_accounts[0]

        try:
            return self.acc_value[account]
        except KeyError:
            pass

        return float()

    def get_acc_cash(self, account=None):
        '''Returns the total cash value sent by TWS during regular updates
        Waits for at least 1 successful download

        If ``account`` is ``None`` then a dictionary with accounts as keys will
        be returned containing all accounts

        If account is specified or the system has only 1 account the dictionary
        corresponding to that account is returned
        '''
        # Wait for at least 1 account update download to have been finished
        # before the cash can be returned to the calling client'
        # if self.connected():
        #     self._event_accdownload.wait()
            # result = [v for v in self.ib.accountValues() \
            #           if v.tag == 'TotalCashBalance' and v.currency == 'BASE']
        # Lock access to acc_cash to avoid an event intefering
            
        #with self._lock_accupd:
        if account is None:
            #wait for the managedAccount Messages
            # if self.connected():
            #     self._event_managed_accounts.wait()

            if not self.managed_accounts:
                return float()

            elif len(self.managed_accounts) > 1:
                return sum(self.acc_cash.values())

            # Only 1 account, fall through to return only 1
            account = self.managed_accounts[0]

        try:
            return self.acc_cash[account]
        except KeyError:
            pass
