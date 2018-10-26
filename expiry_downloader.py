import bs4, requests
import pandas as pd
import os
from tqdm import tqdm
from io import BytesIO
from requests_html import HTMLSession
import json

file = "C:/Users/tomek/zipline/CME_metadata.csv"
downloaded_tables = []
errors = {}

# get contract calendars urls on CME website
f = pd.read_csv(file, parse_dates=['from_date', 'to_date'])
f.drop(f[f['description'].str.contains('Dataset description', regex=False) == True].index, inplace=True)
f['description'] = f['description'].apply(lambda x: x.split('<a href=')[-1].split('>http')[0].strip())
f['description'] = f['description'].str.replace('contract_specifications', 'product_calendar_futures')
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
    Dowload excel file with expiry dates and save on disk.
    """
    # get excel file link
    try:
        session = HTMLSession()
        r = session.get(url)
        r.raise_for_status()
        link = r.html.find('.cmeButtonDownloadExcel', first=True).links.pop()
    except Exception as e:
        errors[root] = (url, e)
        return
    
    
    a = requests.get('https://www.cmegroup.com{}'.format(link))
    a.raise_for_status()
    downloaded_tables.append(root)

    return pd.read_excel(BytesIO(a.content), header=3)

df_list = []
for row in f.iterrows():
    if row[1][7] in downloaded_tables:
        continue
    else:
        df_list.append(excel_downloader(row[1][7], row[1][2]))

df = pd.concat(df_list)
df.columns = map(str.lower, df.columns)
df.rename(columns={'product code': 'exch_symbol'}, inplace=True)  


f = f.merge(df, on='exch_symbol', how='inner')
f.rename(columns={'last trade': 'expiration date'}, inplace=True)
f.drop(['first holding', 'last holding', 'first position', 'last position',
           'first notice', 'last notice', 'first delivery',
           'last delivery'], axis=1, inplace=True)

try:
    f.to_csv('expiry_dates.csv')
except PermissionError:
    print('File expiry_dates.csv is open. Close and try again')

with open('expiry_download_errors.txt', 'w') as f:
    for k, v in errors.items():
        string = '{}: {}\n'.format(k, v)
        f.write(string)
          

    
