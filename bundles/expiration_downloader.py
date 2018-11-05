import os
import sys
import json
import time
import calendar as cal
from io import BytesIO
from datetime import datetime
import requests
from requests_html import HTMLSession
import pandas as pd
from logbook import Logger, FileHandler, StreamHandler
from zipline.assets.futures import CME_CODE_TO_MONTH


stream_handler = StreamHandler(
    sys.stdout,
    format_string=" | {record.message}", bubble=True)
file_handler = FileHandler(
    'expiration_downloader_errors_{}.log'.format(datetime.today().strftime("%Y-%m-%d_%H-%M")),
    format_string=" | {record.message}", bubble=True, delay=True)
log = Logger(__name__)
#stream_handler.push_application()
#file_handler.push_application()


class ExpirationDownloader:
    """
    Download contract expiry dates from CME website.
    Parameters: 
    df: dataframe read from csv file downloaded from quandl
    download: False - use file from disk, True - download file form CME
    show_progress: zipline variable to be passed by caller (or not)

    calling without parameters: use file from disk

    Attributes:
    data: DataFrame with lookup table for expiration dates by contract symbol
    
    """
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    FILENAME = os.path.join(BASE_DIR, 'expiration_dates.csv')
    downloaded_tables = []
    attempts = []
    counter = 0
        
    def __init__(self, df=None, download=False, show_progress=False):
        self.show_progress = show_progress
        if df:
            self.data = df.copy()
        self.router(download)

    def router(self, download):
        """
        Determine whether data should be downloaded or read from disk.
        """
        if download:
            self.get_data()
        else:
            self.data = pd.read_csv(self.FILENAME,
                                        usecols=['expiration_date', 'symbol'],
                                        index_col=['symbol'],
                                        parse_dates=['expiration_date'])
            if self.show_progress:
                log.info('Expiration dates were read from disc.')
        
    def excel_downloader(self, root, url):
        """
        Dowload excel file with expiration dates from CME website.
        """
        # get excel file link
        try:
            session = HTMLSession()
            r = session.get(url)
            r.raise_for_status()
            link = r.html.find('.cmeButtonDownloadExcel', first=True).links.pop()
        except Exception as e:
            log.warn('Failed to download calendar page: {}, error: {}'.format(url, e))
            if r.status_code == 403:
                log.error('CME temporarily blocked your access to their website due to too many requests. Try again in 15 minutes.')
                sys.exit()
            return
        
        a = requests.get('https://www.cmegroup.com{}'.format(link))
        try:
            a.raise_for_status()
            table =  pd.read_excel(BytesIO(a.content), header=3)
            # change root symbols used by CME to Quandl roots
            table['Product Code'] = table['Product Code'].apply(lambda x: root + x[-3:])
            # remember which symbols have already been downloaded to prevent another request
            self.downloaded_tables.append(root)

            return table
        except:
            log.warn('Failed to download excel file: {}, error: {}'.format(link, a.status_code))
    
    def get_specs(self):
        """Process Quandl specs into workable DataFrame.
        """
        df = self.data
        # delete irrelevant data
        df.drop(df[
           df['description'].str.contains('Dataset description', regex=False) == True
        ].index, inplace=True)
        # drop various indexes included in Quandl file
        df.drop(df[df['code'].str.contains('INDEX', regex=False) == True].index, inplace=True)

        # extract url from description field
        df['description'] = df['description'].apply(
            lambda x: x.split('<a href=')[-1].split('>http')[0].strip())

        # change the url to get calendar data instead of contract specs
        df['description'] = df['description'].str.replace(
            'contract_specifications', 'product_calendar_futures')

        # fix an error in urls in quandl meta data file:
        df['description'] = df['description'].str.replace(
            '/mac-swap-futures/', '/swap-futures/')

        #df['year'] = df.code.apply(lambda x: x[-4:])
        df['root_symbol'] = df.code.apply(lambda x: x[:-5])
        df.rename(columns={'code': 'symbol'}, inplace=True)
        df['exch_symbol'] = df['symbol'].apply(lambda x: x[:-4] + x[-2:])
        #df = df[df['year'] > '2018']
        # Filter out non-active contracts (they don't need updating)
        cutoff_date = df['to_date'].max() - pd.Timedelta(days=2)
        df = df[df['to_date'] >= cutoff_date]
        self.data = df


    def get_data(self):
        """
        Get excel tables for all root symbols and process them into workable DataFrame.
        """
        if self.show_progress:
            log.info('Downloading expiration dates from CME website')
        self.get_specs()
        df_list = []
        for row in self.data.iterrows():
            if row[1][6] in self.downloaded_tables:
                continue
            else:
                self.attempts.append(row[1][6])
                self.counter += 1
                if self.counter > 250:
                    # prevent CME website auto-ban
                    time.sleep(10)
                    self.counter = 0
                df_list.append(self.excel_downloader(row[1][6], row[1][2]))

        
        big_df = pd.concat(df_list)
        big_df.columns = map(str.lower, big_df.columns)
        big_df.rename(columns={'product code': 'exch_symbol'}, inplace=True)  

        self.data = self.data.merge(big_df, on='exch_symbol', how='inner')
        self.data.rename(columns={'last trade': 'expiration_date'}, inplace=True)
        self.data['expiration_date'] = self.data['expiration_date'].astype('datetime64[ns]')
        self.data.index = self.data.symbol
        self.data.drop(['first holding', 'last holding', 'first position', 'last position',
                        'first notice', 'last notice', 'first delivery',
                        'last delivery', 'name', 'description', 'refreshed_at',
                        'from_date', 'to_date', 'root_symbol', 'exch_symbol',
                        'contract month', 'first trade', 'settlement', 'symbol'], axis=1, inplace=True)
        self.save_to_file()
                       
    def save_to_file(self):
        try:
            self.data.to_csv(self.FILENAME)
        except PermissionError:
            log.error('File expiration_dates.csv is open. New file will not be saved to disc')
        except:
            log.error('Unknown error. File with expiry dates will not be saved to disc')

          
    @staticmethod
    def third_friday(symbol):
        """
        Return third Friday of the expiration month for the passed symbol.
        Used as a fallback if real expiration date cannot be found.
        """
        year = int(symbol[-4:])
        month = int(CME_CODE_TO_MONTH[symbol[-5]])
        c = cal.Calendar(firstweekday=cal.SATURDAY)
        day = c.monthdatescalendar(year, month)[2][-1]
        return pd.to_datetime(day)


    def get_date(self, symbol):
        """
        Return expiration date. If the date is not available, fall back on using 
        third Friday of the expiration month.
        """
        try:
            return self.data.loc[symbol,'expiration_date']
        except KeyError:
            return self.third_friday(symbol)

