import pandas as pd
import os


spy = pd.read_csv('SPY.csv', parse_dates=['Date'], index_col=['Date'])
spy['returns'] = spy['Adj Close'].pct_change()
spy.drop(['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'], axis=1, inplace=True)
spy.drop(spy.index[0], inplace=True)
spy.index = spy.index.tz_localize('UTC')

user_dir = os.environ.get('USERPROFILE')
target_file = os.path.join(user_dir, '.zipline/data/SPY_benchmark.csv')
spy.to_csv(target_file, header=False)
