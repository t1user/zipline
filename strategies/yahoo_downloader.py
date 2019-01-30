import requests
from fake_useragent import UserAgent
import re
#import pandas as pd
#from io import BytesIO
import datetime


ticker = 'SPY'

ua = UserAgent(verify_ssl=False)
headers = {'User-Agent': ua.chrome}

r = requests.get(
    'https://finance.yahoo.com/quote/SPY/history?p={}'.format(ticker),
    headers=headers)
crumb_regex = r'"CrumbStore":{"crumb":"(.*?)"}'
crumb = re.search(crumb_regex, str(r.content)).group(1)


end = int(datetime.datetime.today().timestamp())
params = {
    'period1': 728262000,
    'period2': end,
    'interval': '1d',
    'events': 'history',
    'crumb': crumb,
}
url = 'https://query1.finance.yahoo.com/v7/finance/download/{}'.format(ticker)
a = requests.get(url,
                 params=params,
                 headers=headers,
                 cookies=r.cookies.get_dict())
filename = a.headers['Content-Disposition'].split('=')[-1]
with open(filename, 'wb') as f:
    f.write(a.content)

"""
df = pd.read_csv(BytesIO(a.content),
                 parse_dates=['Date'],
                 index_col=['Date'],
                 )
df.to_csv('SPY.csv')
"""
