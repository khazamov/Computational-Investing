
'''
@author: Sourabh Bajaj, Omar Khazamov

@summary: Event Profiler 
'''


import pandas as pd
import numpy as np
import math
import copy
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkstudy.EventProfiler as ep



def find_events(ls_symbols, d_data):
    ''' Finding the event dataframe '''
    df_close = d_data['close']
    df_actual_close = d_data['actual_close']    
    ts_market = df_close['SPY']

    print "Finding Events"

    # Creating an empty dataframe
    df_events = copy.deepcopy(df_close)
    df_events = df_events * np.NAN

    # Time stamps for the event range
    ldt_timestamps = df_close.index

    for s_sym in ls_symbols:
        for i in range(1, len(ldt_timestamps)):
            # Calculating the returns for this timestamp
            f_symprice_today = df_close[s_sym].ix[ldt_timestamps[i]]
            f_actual_symprice_today = df_actual_close[s_sym].ix[ldt_timestamps[i]]
            f_symprice_yest = df_close[s_sym].ix[ldt_timestamps[i - 1]]
            f_actual_symprice_yest = df_actual_close[s_sym].ix[ldt_timestamps[i - 1]]
            
            f_marketprice_today = ts_market.ix[ldt_timestamps[i]]
            f_marketprice_yest = ts_market.ix[ldt_timestamps[i - 1]]
            f_symreturn_today = (f_symprice_today / f_symprice_yest) - 1
            f_marketreturn_today = (f_marketprice_today / f_marketprice_yest) - 1
            # Event is found if the symbol is down more then 3% while the
            # market is up more then 2%
            #if f_symreturn_today <= -0.03 and f_marketreturn_today >= 0.02:
            #    df_events[s_sym].ix[ldt_timestamps[i]] = 1
            if f_actual_symprice_yest >= 5.0 and f_actual_symprice_today < 5.0:
                df_events[s_sym].ix[ldt_timestamps[i]] = 1

    return df_events


if __name__ == '__main__':
    dt_start = dt.datetime(2008, 1, 1)
    dt_end = dt.datetime(2009, 12, 31)
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))

    dataobj = da.DataAccess('Yahoo')
    ls_symbols = dataobj.get_symbols_from_list('sp5002012')
    ls_symbols_2008 = dataobj.get_symbols_from_list('sp5002008')
    ls_symbols.append('SPY')
    ls_symbols_2008.append('SPY')
    ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']
    ldf_data = dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys)
    ldf_data_2008 = dataobj.get_data(ldt_timestamps, ls_symbols_2008, ls_keys)
    d_data = dict(zip(ls_keys, ldf_data))
    d_data_2008 = dict(zip(ls_keys, ldf_data_2008))

    for s_key in ls_keys:
        d_data[s_key] = d_data[s_key].fillna(method='ffill')
        d_data[s_key] = d_data[s_key].fillna(method='bfill')
        d_data[s_key] = d_data[s_key].fillna(1.0)
        d_data_2008[s_key] = d_data_2008[s_key].fillna(method='ffill')
        d_data_2008[s_key] = d_data_2008[s_key].fillna(method='bfill')
        d_data_2008[s_key] = d_data_2008[s_key].fillna(1.0)        
    df_events = find_events(ls_symbols, d_data)    
    df_events_2008 = find_events(ls_symbols_2008, d_data_2008)    
    print "Creating Study"

    #ep.eventprofiler(df_events, d_data, i_lookback=20, i_lookforward=20,
    #            s_filename='MyEventStudy.pdf', b_market_neutral=True, b_errorbars=True,
     #           s_market_sym='SPY')
    #ep.eventprofiler(df_events_2008, d_data_2008, i_lookback=20, i_lookforward=20,
    #            s_filename='MyEventStudy2.pdf', b_market_neutral=True, b_errorbars=True,
    #            s_market_sym='SPY')    
