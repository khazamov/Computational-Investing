# homework 4 computational investing
# - feed in results of event profiler to market simulator of hw3

'''
@author: Sourabh Bajaj, Omar Khazamov

@summary: Event Profiler 
'''

import os
import QSTK.qstkutil.qsdateutil as du
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkutil.DataAccess as da
import numpy as np
import csv
import pandas as pd
import datetime as dt
from dateutil.relativedelta import *
import operator
import copy
import math

ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']
    # function for sorting csv file from csv how-to
def sort_by_column(csv_cont, col, reverse=False):
        """ 
        Sorts CSV contents by column name (if col argument is type <str>) 
        or column index (if col argument is type <int>). 
        
        """
        header = csv_cont[0]
        body = csv_cont[1:]
        if isinstance(col, str):  
            col_index = header.index(col)
        else:
            col_index = col
        body = sorted(body, 
               key=operator.itemgetter(col_index), 
               reverse=reverse)
        body.insert(0, header)
        return body
    
    #working with python datetime is more convenient 
    
def insertDT(input_list):
        to_be_sorted = copy.deepcopy(input_list)
        for order_piece in to_be_sorted:
            tickDT = dt.date(int(order_piece[0]),int(order_piece[1]),int(order_piece[2]))
            order_piece[0] = tickDT
        return to_be_sorted
        
    
class Trader:
      
      filepath = ''
      
      def __init__(self, filename, dataprovider, initial_cash):
          self.filepath = filename
          self.cumulativeportval = initial_cash
          self.order_table = []
          self.portfolio = dict
          self.ldt_timestamps = []
          self.tradedates = []
          self.d_data = []
          self.daily_portfolio_val = []
          self.ls_symbols = []
          
          dt_start = dt.datetime(2008, 1, 1)
          dt_end = dt.datetime(2009, 12, 31)
          ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))
      
          self.dataobj = da.DataAccess(dataprovider)
          self.ls_symbols = self.dataobj.get_symbols_from_list('sp5002012')
          #ls_symbols_2008 = dataobj.get_symbols_from_list('sp5002008')
          self.ls_symbols.append('SPY')
          #ls_symbols_2008.append('SPY')
          
          ldf_data = self.dataobj.get_data(ldt_timestamps, self.ls_symbols, ls_keys)
          #ldf_data_2008 = dataobj.get_data(ldt_timestamps, ls_symbols_2008, ls_keys)
          self.d_data = dict(zip(ls_keys, ldf_data))             
          
      def process_data(self):
          
          dt_timeofday = dt.timedelta(hours=16)
          try:
           with open(self.filepath,'rU') as csv_con:
              readerlist = list(csv.reader(csv_con, delimiter = ",", quoting = csv.QUOTE_NONE))
              #convert strings to python datetimes
              reader_listDT = insertDT(readerlist)
              #sort in chronological order so we can prepare neat orders table
              #sorted(reader_listDT, key = lambda ticker: ticker[0]) - this is a not joke
              reader_listDT.sort(key = lambda ticker: ticker[0])
              for row in reader_listDT:
                  print row, '\n'
              #getting set of tickers from trade book
              portfolio = []
              for row in  reader_listDT:
                  portfolio.append(row[3])
                  #took out from program method to place in initialisation phase
                  self.tradedates.append(row[0]) 
              portfolioset = set(portfolio)
              #timestamps for given trade period
              self.ldt_timestamps = du.getNYSEdays(reader_listDT[0][0], reader_listDT[-1][0], dt.timedelta(hours=16))
              ldf_data = self.dataobj.get_data(self.ldt_timestamps, list(portfolioset), ls_keys)
              #after zipping up with ls_keys, it's basically a dictionary of dictionary
              d_data = dict(zip(ls_keys, ldf_data))
              #need order table to run simulation
              self.order_table = copy.deepcopy(reader_listDT)
              #initialize portfolio allocation
              self.portfolio = {x: 0 for x in portfolioset}
              print self.portfolio     
          except Exception, e: 
               print e 
           
  
      def find_events(self):
                   ''' Finding the event dataframe '''
                   df_close = self.d_data['close']
                   df_actual_close = self.d_data['actual_close']    
                   ts_market = df_actual_close['SPY']
               
                   print "Finding Events"
                   # separate out event related parameters
                   # from transcation parameters
                   #
                   number_of_shares = 100
                   # Creating an empty dataframe
                   df_events = copy.deepcopy(df_actual_close)
                   df_events = df_events * np.NAN
                   f = open(self.filepath, 'wb')
                   eventsfile = csv.writer(f, delimiter = ',' , quotechar = '\'', quoting = csv.QUOTE_MINIMAL)
                   
                   # Time stamps for the event range
                   self.ldt_timestamps = df_actual_close.index
                   i = 0    
                   for s_sym in self.ls_symbols:
                       for i in range(1, len(self.ldt_timestamps)):
                           # Calculating the returns for this timestamp
                           f_symprice_today = df_actual_close[s_sym].ix[self.ldt_timestamps[i]]
                           f_actual_symprice_today = df_actual_close[s_sym].ix[self.ldt_timestamps[i]]
                           f_symprice_yest = df_actual_close[s_sym].ix[self.ldt_timestamps[i - 1]]
                           f_actual_symprice_yest = df_actual_close[s_sym].ix[self.ldt_timestamps[i - 1]]
                           
                           f_marketprice_today = ts_market.ix[self.ldt_timestamps[i]]
                           f_marketprice_yest = ts_market.ix[self.ldt_timestamps[i - 1]]
                           f_symreturn_today = (f_symprice_today / f_symprice_yest) - 1
                           f_marketreturn_today = (f_marketprice_today / f_marketprice_yest) - 1
                           # Event is found if the symbol is down more then 3% while the
                           # market is up more then 2%
                           #if f_symreturn_today <= -0.03 and f_marketreturn_today >= 0.02:
                           #    df_events[s_sym].ix[ldt_timestamps[i]] = 1
                           
                           if f_actual_symprice_yest >= 5.0 and f_actual_symprice_today < 5.0:
                               #df_events[s_sym].ix[ldt_timestamps[i]] = 1 
                               buy_year = self.ldt_timestamps[i].year
                               buy_month = self.ldt_timestamps[i].month
                               buy_day = self.ldt_timestamps[i].day
                               idx = i+5
                               if i+5 >= len(self.ldt_timestamps) :
                                   idx = -1
                               sell_year = self.ldt_timestamps[idx].year
                               sell_month = self.ldt_timestamps[idx].month
                               sell_day = self.ldt_timestamps[idx].day
                               enter_trade = [buy_year] +[buy_month] + [buy_day] + [s_sym] + ['BUY'] + [number_of_shares]
                               exit_trade = [sell_year] +[sell_month] + [sell_day] + [s_sym] + ['SELL'] + [number_of_shares]
                               eventsfile.writerows([enter_trade, exit_trade])
                               
                   f.close()     
                   
      def run(self):
          i=0
          daily_portfolio_val = []
          #print d_data
          close_prices = self.d_data['close'].copy()
          close_prices = close_prices.fillna(method='ffill')
          close_prices = close_prices.fillna(method='bfill')            
          tradedates = []
          #idea is loop through every single day and then, check an order book if the date is in there
          for time in self.ldt_timestamps: 
              cash_value = 0
              datestr = str(time).rsplit(' ')[0].rsplit('-')
              datestamp = dt.date(int(datestr[0]),int(datestr[1]), int(datestr[2]))
          
              samedaytrade = self.tradedates.count(datestamp) 
              
              #handling multiple trades made on the same day 
              if samedaytrade!= 0:
                 for i in range(0, samedaytrade):
                   trade_index = self.tradedates.index(datestamp) + i 
                   order = self.order_table[trade_index]
                   if order[4].upper() ==  'BUY':
                       flow = -1
                       self.portfolio[order[3]] += int(order[5])
                   if order[4].upper() == 'SELL':
                       flow = 1
                       self.portfolio[order[3]] -= int(order[5]) 
                   adj_value = int(order[5]) * close_prices[order[3]][time]
                   #instead of maintaining cash entry in portfolio just use difference between initial cash value and present day portfolio cash value
                   self.cumulativeportval = self.cumulativeportval + adj_value * flow
                   #rebalancing according to daily market fluctuations
                   #important to keep rebalancing outside of the trade orders loop
                   #otherwise daily returns will have multiple entries for the same day and computestats() will return erroneous results
              for ticker in self.portfolio:
                    cash_value +=  self.portfolio[ticker] * close_prices[ticker][time]
              self.daily_portfolio_val.append(self.cumulativeportval  + cash_value)
              print time, self.portfolio, self.cumulativeportval  + cash_value
                 
      def computestats(self):          
             #nothing fancy, just use library functions to compute daily returns, standard deviation and average of daily returns
             self.daily_portfolio_val /= self.daily_portfolio_val[0]
             daily_portfolio_return = tsu.returnize0(self.daily_portfolio_val)
             avg = np.mean(daily_portfolio_return) 
             stdev = np.std(daily_portfolio_return)
             SR = avg / stdev * math.sqrt(252)   
             total_return = 1
             for ret in range(1,len(daily_portfolio_return)):
                 
                 total_return = total_return * (1 + daily_portfolio_return[ret])
             
             
             print "AVG return: ",avg, "stdev of return: ", stdev, "sharp ratio: ", SR , "total cumulative return", total_return      
          
                
def main():
     

        rstrategy = Trader('D:\\orders.csv', 'Yahoo', 50000)
        rstrategy.process_data()        
        rstrategy.find_events()
        
        rstrategy.run()
        rstrategy.computestats()
        
        print rstrategy.order_table
if __name__ == '__main__':
        main()
