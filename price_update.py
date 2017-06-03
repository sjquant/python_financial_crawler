# 가격 업데이트하는 클래스

from kiwooma.spider import Spider
import sqlite3
import pandas as pd
from datetime import datetime
import time

class PriceUpdate(object):

    def __init__(self):
        # Spider Class
        self.spider = Spider()
        # DB연결
        self.con = sqlite3.connect('E:/Coding/db_from_csv/financial_info.db')
        self.cursor = self.con.cursor()

    #get total code
    def get_total_codes(self):
        kospi_codes = self.spider.get_code_list_by_market('kospi')
        kosdaq_codes = self.spider.get_code_list_by_market('kosdaq')
        etf_codes = self.spider.get_code_list_by_market('etf')
        total_codes = kospi_codes + kosdaq_codes + etf_codes
        return total_codes
    
    #update daily price
    def update_daily_price(self):
        start_time = time.time() # temporary: used for recoding time elapsed

        for index, code in enumerate(self.get_total_codes()):
            if code[-1] != '0': # Exclude Non Ordinary Shares
                continue 
            Acode = 'A' + code # change code name according to the code name in table
            print('A' + code)

            updated_date = self._last_date_in_table(Acode, 'PriceInfo')
            try:           
                if datetime.today().date() == updated_date.date() + pd.Timedelta(days=1): # updated date in table
                    print(Acode + ' already updated.')
                    continue
            except AttributeError: # When updated_date is None
                pass
                
            if updated_date == None: #If there is not this code data in the table
                sinfo = self.spider.get_daily_ohlcv(code, repeat = True, adj_close = 1)
                 
            else:
                sinfo = self.spider.get_daily_ohlcv(code, adj_close = 1) #get daily ohlcv 
        
            # If the volume of the previous days was 0, update adj_close
            try:
                if True in sinfo[sinfo['Date'] > updated_date]['Volume'] == 0: # sinfo['Volume'][-2] == 0 and sinfo['Volume'][-1] != 0: #전날 거래량이 0, 당일 거래량이 0이 아닌경우
                    print('Updating Adj Close')
                    sinfo = self._get_adj_close(code) 
                
            except IndexError: # If a stock was listed recently
                pass
                
            sinfo['Code'] = Acode # Add Code Column
            sinfo.reset_index(inplace = True)
            sinfo.set_index('Code', inplace = True) #reset index
            if updated_date != None:
                sinfo = sinfo.loc[sinfo['Date'] > updated_date] # get data which was not updated
            sinfo.to_sql('PriceInfo', self.con, if_exists='append')
            time.sleep(1)
           
            time_taken = time.time() - start_time
            print(str(index + 1) + "--- %s seconds ---" % time_taken) #print index and time elapsed
            if time_taken > 1000: # exit program if time taken > 1000
                exit()

    # get last_updated date in table
    def _last_date_in_table(self, code, table):
        '''
        return last date from table
        '''
        try:
            original_table = pd.read_sql('SELECT Date, Code FROM %s WHERE Code = "%s"' % (table, code), self.con, index_col = 'Date')
            original_table.index = pd.to_datetime(original_table.index, format = '%Y-%m-%d')
            original_table.sort_index(inplace=True)
            last_date = original_table.index[-1]
            return last_date
        except IndexError: #해당 데이터의 테이블이 없는 경우
            print('No Data')
            return None

    #delete code data from the table and get new data
    def _get_adj_close(self, code):
        #Delete Table
        self.cursor.execute('DELETE FROM PriceInfo WHERE Code = "%s"' % code)
        self.con.commit()

        sinfo = self.spider.get_daily_ohlcv(code, repeat = True, adj_close = 1)
              
        return sinfo

    #update minutely price
    def update_minutely_price(self):
        start_time = time.time() # temporary: used for recoding time elapsed
        
        for index, code in enumerate(self.get_total_codes()):
            
            if code[-1] != '0': # Exclude Non Ordinary Shares
                continue 
            Acode = 'A' + code # change code name according to the code name in table
            print('A' + code)

            updated_date = self._last_date_in_table(Acode, 'MinutePriceInfo')
            try:           
                if datetime.today().date() == updated_date.date() + pd.Timedelta(days=1): # updated date in table
                    print(Acode + ' already updated.')
                    continue
            except AttributeError: # When updated_date is None
                pass
                
            if updated_date == None: #If there is not this code data in the table
                sinfo = self.spider.get_minutely_ohlcv(code, '1', repeat = True, repeat_cnt = 15)
            else:
                sinfo = self.spider.get_minutely_ohlcv(code, '1') #get minutely ohlcv 
      

            sinfo['Code'] = Acode # Add Code Column
            sinfo.reset_index(inplace = True)
            sinfo.set_index('Code', inplace = True) #reset index
            if updated_date != None:
                sinfo = sinfo.loc[sinfo['Date'] > updated_date] # get data which was not updated
            sinfo.to_sql('MinutePriceInfo', self.con, if_exists='append')
            time.sleep(1)
            
            time_taken = time.time() - start_time
            print(str(index + 1) + "--- %s seconds ---" % time_taken) #print index and time elapsed
            if time_taken > 1000: # exit program if time taken > 1000
                exit()