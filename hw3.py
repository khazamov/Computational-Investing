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
  
  def __init__(self, filename, initial_cash):
      self.filepath = filename
      self.cumulativeportval = initial_cash
      self.order_table = []
      self.portfolio = dict
      self.ldt_timestamps = []
      self.tradedates = []
      self.d_data = []
      self.daily_portfolio_val = []
      
      
  def read_data(self, dataprovider):
      #find out what symbols are there
      c_dataobj = da.DataAccess(dataprovider)
      ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']
      dt_timeofday = dt.timedelta(hours=16)
      try:
       with open(self.filepath,'rU') as csv_con:
          readerlist = list(csv.reader(csv_con, delimiter = ",", quoting = csv.QUOTE_NONE))
          #convert strings to python datetimes
          reader_listDT = insertDT(readerlist)
          #sort in chronological order so we can prepare neat orders table
          sorted(reader_listDT, key = lambda ticker: ticker[0]) 
          #getting set of tickers from trade book
          portfolio = []
          for row in  reader_listDT:
              portfolio.append(row[3])
              #took out from program method to place in initialisation phase
              self.tradedates.append(row[0]) 
          portfolioset = set(portfolio)
          #timestamps for given trade period
          self.ldt_timestamps = du.getNYSEdays(reader_listDT[0][0], reader_listDT[-1][0], dt.timedelta(hours=16))
          ldf_data = c_dataobj.get_data(self.ldt_timestamps, list(portfolioset), ls_keys)
          #after zipping up with ls_keys, it's basically a dictionary of dictionary
          self.d_data = dict(zip(ls_keys, ldf_data))
          #need order table to run simulation
          self.order_table = copy.deepcopy(reader_listDT)
          #initialize portfolio allocation
          self.portfolio = {x: 0 for x in portfolioset}
                
      except Exception, e: 
           print e 


  # don't forget self!
  # goes through order book and puts the trade, rebalances portfolio
  def program(self):
      i=0
      daily_portfolio_val = []
      #print d_data
      close_prices =self.d_data['close'].copy()
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
               #instead of maintaining cash entry in portfolio just have difference between initial cash and present day portfolio cash value
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
         daily_portfolio_return = tsu.returnize0(self.daily_portfolio_val)
         avg = np.mean(daily_portfolio_return) 
         stdev = np.std(daily_portfolio_return)
         SR = avg / stdev * math.sqrt(252)   
         print "AVG return: ",avg, "stdev of return: ", stdev, "sharp ratio: ", SR       
      
            
def main():
    
    rstrategy = Trader('D:\\coursera\\computational investing\\orders2.csv', 1000000)
    # has to be changed
    rstrategy.read_data('Yahoo')
    rstrategy.program()
    rstrategy.computestats()
    print rstrategy.order_table
if __name__ == '__main__':
    main()
