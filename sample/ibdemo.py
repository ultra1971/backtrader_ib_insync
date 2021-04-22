#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2021 Daniel Rodriguez
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

import backtrader as bt
import datetime

import backtrader_ib_insync as ibnew

Status = [
        'Created', 'Submitted', 'Accepted', 'Partial', 'Completed',
        'Canceled', 'Expired', 'Margin', 'Rejected',
        ]

class Test(bt.Strategy):
    
    def __init__(self):
        self.data_live = False
        
    def notify_data(self, data, status, *args, **kwargs):
        print('*' * 5, 'DATA NOTIF:', data._getstatusname(status), *args)
        if status == data.LIVE:
            self.data_live = True

    def notify_order(self, order):
        print("OrderStatus", Status[order.status])
        if order.status == order.Completed:
            buysell = 'BUY Completed' if order.isbuy() else 'SELL Completed'
            txt = '{} {}@{}'.format(buysell, order.executed.size,
                                    order.executed.price)
            print(txt)

    def next(self):
        txt = []
        #txt.append('{}'.format(len(self)))
        txt.append('{}'.format(self.data.datetime.datetime(0).isoformat()))
        txt.append('{:.6f}'.format(self.data.open[0]))
        txt.append('{:.6f}'.format(self.data.high[0]))
        txt.append('{:.6f}'.format(self.data.low[0]))
        txt.append('{:.6f}'.format(self.data.close[0]))
        txt.append('{:.0f}'.format(self.data.volume[0]))
        print(','.join(txt))

        if not self.data_live:
            return

        if len(self) % 2 == 0:
            print ('ORDER BUY Created')
            self.buy(size = 3000)
            
        if len(self) % 2 == 1:
            print ('ORDER SELL Created')
            self.sell(size = 3000)

def run(args=None):
    cerebro = bt.Cerebro()

    start = datetime.datetime(2020, 4, 1)
    end = datetime.datetime(2020, 5, 28)
    
    storekwargs = dict(
            host= 'localhost', port=7497,
            clientId=None, 
            account=None,
            timeoffset=True,
            reconnect=3, timeout=3.0,
            notifyall=True, 
            _debug=False,
            )

    print ("Using New IBstore")

    ibstore = ibnew.IBStore(**storekwargs)
    
    broker = ibstore.getbroker()
    cerebro.setbroker(broker)
    
    ibdata = ibstore.getdata

    datakwargs = dict(
                timeframe= bt.TimeFrame.TFrame("Seconds"), compression=1,
                historical=False, fromdate=start, todate = end,   
                rtbar=False,
                qcheck=0.5,
                what=None,
                backfill_start=True, backfill=True,
                latethrough=True,
                tz=None,
                useRTH = False,
                hist_tzo = None,
                )
    
    data0 = ibdata(dataname='EUR.USD-CASH-IDEALPRO', **datakwargs)

    print("Data added to cerebro")
    #cerebro.resampledata(data0, timeframe=bt.TimeFrame.Seconds, compression=10)
    cerebro.adddata(data0)
       
    cerebro.addstrategy(Test)
    
    cerebro.run()

if __name__ == '__main__':
    run()
