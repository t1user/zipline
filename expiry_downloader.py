import bs4, requests
import pandas as pd
import os
from tqdm import tqdm

file = "C:/Users/tomek/zipline/CME_metadata.csv"

# get contract calendars urls on CME website
f = pd.read_csv(file)
f.drop(f[f['description'].str.contains('Dataset description', regex=False) == True].index, inplace=True)
f['description'] = f['description'].apply(lambda x: x.split('<a href=')[-1].split('>http')[0])
f['description'] = f['description'].str.replace('contract_specifications', 'product_calendar_futures')
f['year'] = f.code.apply(lambda x: x[-4:])
f['root_symbol'] = f.code.apply(lambda x: x[:-5])
f.rename(columns={'code': 'symbol'}, inplace=True)
f['exch_symbol'] = f['symbol'].apply(lambda x: x[:-4] + x[-2:])
f = f[f['year'] > '2017']
f.to_csv('CME_meta_with_urls.csv')

if 'excel_meta' not in os.listdir():
    os.mkdir('excel_meta')

def excel_downloader(root, url):
    """
    Dowload excel file with expiry dates and save on disk.
    """
    if '{}.xls'.format(root) in os.listdir('excel_meta'):
        return
    # get excel file link
    try:
        r = requests.get(url)
        r.raise_for_status()
    except:
        return
    soup = bs4.BeautifulSoup(r.text, "html.parser")
    try:
        link = soup.find('a', attrs={'class': 'cmeButtonDownloadExcel'}).get('href')
    except:
        return
    a = requests.get('https://www.cmegroup.com{}'.format(link))
    with open('excel_meta/{}.xls'.format(root), 'wb') as f:
        f.write(a.content)


for row in tqdm(f.iterrows()):
    excel_downloader(row[1][7], row[1][2])


print('Downloaded: {} files, failed to download files for {} symbols.'.format(
    len(os.listdir('excel_meta')),
    len(f.root_symbol.unique()) - len(os.listdir('excel_meta'))
))

df_list = [pd.read_excel(os.path.join('excel_meta', _f), header=3)
           for _f in os.listdir('excel_meta')]
df = pd.concat(df_list)
df.columns = map(str.lower, df.columns)
df.rename(columns={'product code': 'exch_symbol'}, inplace=True)  


f = f.merge(df, on='exch_symbol', how='inner')
f.rename(columns={'last trade': 'expiration date'}, inplace=True)
f.drop(['first holding', 'last holding', 'first position', 'last position',
           'first notice', 'last notice', 'first delivery',
           'last delivery'], axis=1, inplace=True)
f.to_csv('expiry_dates.csv')
          

    
