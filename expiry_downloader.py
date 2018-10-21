import bs4, requests
import pandas as pd
import os


file = "C:/Users/tomek/zipline/CME_metadata.csv"

# get contract calendars urls on CME website
f = pd.read_csv(file)
f.drop(f[f['description'].str.contains('Dataset description', regex=False) == True].index, inplace=True)
f['description'] = f['description'].apply(lambda x: x.split('<a href=')[-1].split('>http')[0])
f['description'] = f['description'].str.replace('contract_specifications', 'product_calendar_futures')
f['year'] = f.code.apply(lambda x: x[-4:])
f['root'] = f.code.apply(lambda x: x[:-5])
f = f[f['year'] > '2017']
f.to_csv('CME_meta_with_urls.csv')

if 'excel_meta' not in os.listdir():
    os.mkdir('excel_meta')

def excel_downloader(root, url):
    """
    Dowload excel file with expiry dates and save on disk.
    """
    counter = 0
    if '{}.xls'.format(root) in os.listdir('excel_meta'):
        return
    # get excel file link
    try:
        r = requests.get(url)
        r.raise_for_status()
    except:
        counter += 1
        print('Failed: {}'.format(root))
        return
    soup = bs4.BeautifulSoup(r.text, "html.parser")
    try:
        link = soup.find('a', attrs={'class': 'cmeButtonDownloadExcel'}).get('href')
    except:
        counter += 1
        print('Failed: {}'.format(root))
        return
    a = requests.get('https://www.cmegroup.com{}'.format(link))
    with open('excel_meta/{}.xls'.format(root), 'wb') as f:
        f.write(a.content)
        print('Wrote file: {}.csv'.format(root))

for row in f.iterrows():
    excel_downloader(row[1][7], row[1][2])


    
