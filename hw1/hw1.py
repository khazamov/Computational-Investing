#portfolio optimization

import QSTK.qstkutil.qsdateutil as du
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkutil.DataAccess as da
import numpy as np
import math
import random
import datetime as dt
import matplotlib.pyplot as plt
import pandas as pd



def simulate(startdate, enddate, ls_symbols, weights):
 dt_start = startdate
 dt_end = enddate
 dt_timeofday = dt.timedelta(hours=16)
 ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)
 c_dataobj = da.DataAccess('Yahoo')
 ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']
 ldf_data = c_dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys)
 d_data = dict(zip(ls_keys, ldf_data))
 SR = {}
#na_normalized_price = na_price / na_price[0, :]


 
 close_prices = d_data['close'].copy()
 #open_prices = d_data['open'].values
 close_prices= close_prices.fillna(method='ffill')
 close_prices = close_prices.fillna(method='bfill') 
 na_rets = close_prices.values
 na_rets = na_rets / na_rets[0,:]
 for i in range(0, len(ls_symbols)-1):
  na_rets[:,i] = na_rets [:,i] * weights[i] 
 daily_portfolio_val = np.sum(na_rets, axis =1)
 daily_portfolio_return = tsu.returnize0(daily_portfolio_val)
 avg = np.mean(daily_portfolio_return) 
 stdev = np.std(daily_portfolio_return)

 #na_rets = na_rets / na_rets[0,:]
 
    

 SR = avg / stdev
 return SR

def isLegal(weights):
  if (np.sum(weights) == 1):
    return True
  return False


#helper to range through floats
def xfrange(start, stop, step):
    while start < stop:
        yield start
        start += step
        
#BRUTEFORCE OPTIMIZATION        
#TODO: optimization with gradient ascent
def optimize():
  hSR = 0
  bestPF = [0.2,0.2,0.2,0.4]
  for A in xfrange(0.0,1.0,0.1):
    for G in xfrange(0.0,1.0,0.1):
      for GOO in xfrange(0.0,1.0,0.1):
        for X in xfrange(0.0,1.0,0.1):
          weights = np.array([A,G,GOO,X])
          if isLegal(weights):  
            SR = simulate(dt.date(2010,1,1), dt.date(2010,12,31),   ['AXP', 'HPQ', 'IBM', 'HNZ'] , [A, G, GOO, X])
            if SR > hSR:
              hSR = SR
              bestPF = weights
  return hSR, bestPF
 
 #GRADIENT ASCENT
 
# def gradient_ascent
#   weights=[0.2,0.4, 0.5,0.6]
#   SR = simulate(dt.date(2010,1,1), dt.date(2010,12,31),  ['AXP', 'HPQ', 'IBM', 'HNZ'], [0.0, 0.0, 0.0, 1.0])
#   squared_diff
