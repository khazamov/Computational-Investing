import os
import QSTK.qstkutil.qsdateutil as du
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkutil.DataAccess as da
import csv
import pandas as pd
import datetime as dt
from dateutil.relativedelta import *
import operator
import copy


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
  def program(self,dataprovider):
    print "Running simulator"
    c_dataobj = da.DataAccess(dataprovider)
    ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']
    dt_timeofday = dt.timedelta(hours=16)
    try:
     with open(self.filepath,'rU') as csv_con:
        readerlist = list(csv.reader(csv_con, delimiter = ",", quoting = csv.QUOTE_NONE))
        reader_listDT = insertDT(readerlist)
        sorted(reader_listDT, key = lambda ticker: ticker[0]) 
        
        
        
        #print reader_listDT
        for row in reader_listDT:
            #print '---------',row,'----------'
            #get_data method:
            #passing a string instead of a list gives  quite confusing exception message - FILE NOT FOUND.
            #use split() to convert the string to list collection
            ldt_timestamps = du.getNYSEdays(row[0] , row[0] , dt.timedelta(hours=16))
            ldf_data = c_dataobj.get_data(ldt_timestamps, row[3].split(), ls_keys)
            d_data = dict(zip(ls_keys, ldf_data))
            d_dataval = d_data['close'].copy()
            na_rets = d_dataval.values            
            daily_delta = (na_rets[0] )* int(row[5])
            if (row[4].upper() =='BUY'):
              self.cumulativeportval = self.cumulativeportval - daily_delta
            if (row[4].upper() =='SELL'):
                self.cumulativeportval = self.cumulativeportval + daily_delta
            print row, self.cumulativeportval, '\r'      
            
    except Exception, e: 
        print e       
       
       

def main():
    rstrategy = Trader('D:\\coursera\\computational investing\\test.csv', 1000000)
    rstrategy.program('Yahoo')
    #print rstrategy.cumulativeportval
        
if __name__ == '__main__':
    main()
