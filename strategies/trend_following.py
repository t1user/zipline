import pandas as pd
import numpy as np
import talib
from zipline.api import order, record, symbol, continuous_future, future_symbol
from contracts import contracts


FAST_MA = 50
SLOW_MA = 100
BREAKOUT = 20
STOP = 2

def initialize(context):
    context.cont_contracts = [
        continuous_future(contract,
                          offset=0,
                          adjustment='mul',
                          roll='volume')
        for contract in contracts]

        

def handle_data(context, data):
    df = data.history(context.cont_contracts,
                      fields = ['price', 'high', 'low'],
                      bar_count = SLOW_MA + 10,
                      frequency = '1d')

    print(df.head())



