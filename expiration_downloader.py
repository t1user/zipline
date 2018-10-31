import os
import sys
import json
from io import BytesIO
from datetime import datetime
import requests
from requests_html import HTMLSession
import pandas as pd
from tqdm import tqdm
from logbook import Logger, FileHandler, StreamHandler



if 'logs' not in os.listdir():
    os.mkdir('logs')

stream_handler = StreamHandler(
    sys.stdout,
    format_string=" | {record.message}", bubble=True)
file_handler = FileHandler(
    'logs/expiration_downloader_errors_{}.log'.format(datetime.today().strftime("%Y-%m-%d_%H-%M")),
    format_string=" | {record.message}", bubble=True)
log = Logger(__name__)
#stream_handler.push_application()
file_handler.push_application()


file = "C:/Users/tomek/zipline/CME_metadata.csv"
downloaded_tables = []

# get contract calendars urls on CME website
f = pd.read_csv(file, parse_dates=['from_date', 'to_date'])
f.drop(f[
    f['description'].str.contains('Dataset description', regex=False) == True
].index, inplace=True)

# extract url only from description field
f['description'] = f['description'].apply(
    lambda x: x.split('<a href=')[-1].split('>http')[0].strip())

# change the url to get calendar data instead of contract specs
f['description'] = f['description'].str.replace(
    'contract_specifications', 'product_calendar_futures')

# fix an error in urls in quandl meta data file:
f['description'] = f['description'].str.replace(
    '/mac-swap-futures/', '/swap-futures/')

f['year'] = f.code.apply(lambda x: x[-4:])
f['root_symbol'] = f.code.apply(lambda x: x[:-5])
f.rename(columns={'code': 'symbol'}, inplace=True)
f['exch_symbol'] = f['symbol'].apply(lambda x: x[:-4] + x[-2:])
f = f[f['year'] > '2017']
# Filter out non-active contracts (which don't need updating)
d = f['to_date'].max() - pd.Timedelta(days=2)
f = f[f['to_date'] >= d]

f.to_csv('CME_meta_with_urls.csv')


def excel_downloader(root, url):
    """
    Dowload excel file with expiration dates and save on disk.
    """
    # get excel file link
    try:
        session = HTMLSession()
        r = session.get(url)
        r.raise_for_status()
        link = r.html.find('.cmeButtonDownloadExcel', first=True).links.pop()
    except Exception as e:
        log.warn('Failed to download calendar page: {}, error: {}'.format(url, e))
        return
    
    
    a = requests.get('https://www.cmegroup.com{}'.format(link))
    try:
        a.raise_for_status()
    except:
        log.warn('Failed to download excel file: {}, error: {}'.format(link, a.status_code))
    downloaded_tables.append(root)

    return pd.read_excel(BytesIO(a.content), header=3)

    
df_list = []
for row in tqdm(f.iterrows()):
    if row[1][7] in downloaded_tables:
        continue
    else:
        df_list.append(excel_downloader(row[1][7], row[1][2]))
        
df = pd.concat(df_list)
df.columns = map(str.lower, df.columns)
df.rename(columns={'product code': 'exch_symbol'}, inplace=True)  


f = f.merge(df, on='exch_symbol', how='inner')
f.rename(columns={'last trade': 'expiration_date'}, inplace=True)
f.drop(['first holding', 'last holding', 'first position', 'last position',
           'first notice', 'last notice', 'first delivery',
           'last delivery'], axis=1, inplace=True)

try:
    f.to_csv('expiration_dates.csv')
except PermissionError:
    log.error('File expiration_dates.csv is open. Close and try again')
          

    
