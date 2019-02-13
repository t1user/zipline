# WORK IN PROGRESS

import pandas as pd
import numpy as np
import talib
import sys
from datetime import datetime
from logbook import Logger, StreamHandler, FileHandler
from zipline.api import (order, record, symbol, continuous_future,
                         future_symbol, get_open_orders, order_target_percent,
                         set_slippage, set_commission, get_datetime,
                         schedule_function, date_rules, time_rules)
from zipline.finance.slippage import FixedSlippage, SlippageModel
from zipline.finance.commission import PerTrade
from contracts import contracts
import pdb


stream_handler = StreamHandler(
    sys.stdout, format_string=" | {record.message}", bubble=True)
# file_handler = FileHandler('strategy_1_{}.log'.format(
#    datetime.today().strftime("%Y-%m-%d_%H-%M")),
#    format_string=" | {record.message}", bubble=True)
log = Logger(__name__)
# file_handler.push_application()
# stream_handler.push_application()


FAST_MA = 25
SLOW_MA = 100
BREAKOUT = 50  # breakout beyond x days max/min
STOP = 2  # stop after x ATRs
RISK = .2  # % of capital daily risk per position


class InstantSlippage(SlippageModel):
    """
    Workaround to trade at openning rather than closing prices.
    """

    def process_order(self, data, order):
        # Use price from previous bar
        price = data.history(order.sid, 'open', 1, '1d').fillna(
            data.history(order.sid, 'price', 2, '1d')[0])[-1]
        return (price, order.amount)


def initialize(context):
    set_slippage(us_futures=InstantSlippage())
    set_commission(us_futures=PerTrade(0))
    context.contracts = [
        continuous_future(contract,
                          offset=0,
                          adjustment='mul',
                          roll='volume')
        for contract in contracts]
    context.min_max = {}


def handle_data(context, data):
    valid_contracts = [contract for contract in context.contracts
                       if contract.start_date <= get_datetime() - pd.Timedelta(days=SLOW_MA+2)
                       and contract.end_date >= get_datetime()]
    hist = data.history(valid_contracts,
                        fields=['price', 'high', 'low'],
                        bar_count=SLOW_MA + 1,
                        frequency='1d')

    slow_ma = hist['price'].apply(lambda x:
                                  talib.EMA(x.as_matrix(),
                                            timeperiod=SLOW_MA)[-1])
    fast_ma = hist['price'].apply(lambda x:
                                  talib.EMA(x.as_matrix(),
                                            timeperiod=FAST_MA)[-1])
    atr = hist.apply(lambda x: talib.ATR(x['high'].fillna(x['price']).as_matrix(),
                                         x['low'].fillna(
                                             x['price']).as_matrix(),
                                         x['price'].as_matrix(),
                                         timeperiod=SLOW_MA)[-1],
                     axis=(1, 0))
    # breakout above is a buy signal
    upper = hist['price'][-BREAKOUT-1:-2].max(axis=0)
    # breakout below is a sell signal
    lower = hist['price'][-BREAKOUT-1:-2].min(axis=0)
    # last price
    price = hist['price'].fillna(method='ffill').iloc[-1]

    # position sizes as % of portfolio value
    weights = RISK/100 * price/atr

    longs = ((price > upper) & (fast_ma > slow_ma)) * weights
    shorts = ((price < lower) & (fast_ma < slow_ma)) * -weights
    signals = longs + shorts

    # convert Continuous_future objects in indexes to current Future objects
    signals.index = data.current(signals.index, 'contract')
    atr.index = data.current(atr.index, 'contract')
    price.index = data.current(price.index, 'contract')
    signals = signals[signals != 0].dropna()

    # rollover expiring contracts
    for cont, pos in context.portfolio.positions.items():
        if cont not in price.index:
            weight = context.portfolio.current_portfolio_weights[cont]
            root = cont.root_symbol
            # close existing contract
            signals[cont] = 0
            current = data.current(continuous_future(root), 'contract')
            position = context.portfolio.positions[cont]
            # open current contract only if unrealised PnL positive
            if (position.last_sale_price - position.cost_basis) * position.amount > 0:
                if current:  # contract could've been delisted
                    signals[current] = weight
            # still need the old contract for stop-loss calculation
            atr[cont] = atr[current]
            price[cont] = price[current]

    # implement stop-loss
    for contract, position in context.portfolio.positions.items():
        if contract in context.min_max:
            context.min_max[contract] = (min(context.min_max[contract][0],
                                             position.cost_basis,
                                             position.last_sale_price),
                                         max(context.min_max[contract][1],
                                             position.cost_basis,
                                             position.last_sale_price))
        else:
            context.min_max[contract] = (min(position.cost_basis,
                                             position.last_sale_price),
                                         max(position.cost_basis,
                                             position.last_sale_price))

        # calculate stop loss level
        if position.amount > 0:
            stop_price = context.min_max[contract][1] - \
                atr[contract].item() * STOP
            if price[contract] <= stop_price:
                signals[contract] = 0
                del context.min_max[contract]
        if position.amount < 0:
            stop_price = context.min_max[contract][0] + \
                atr[contract].item() * STOP
            if price[contract] >= stop_price:
                signals[contract] = 0
                del context.min_max[contract]

    # execute trades
    existing_positions = list(context.portfolio.positions.keys())
    for asset, target in signals.items():
        # pdb.set_trace()
        log.info('transactions on {}:'.format(get_datetime()))
        if asset in existing_positions:
            # don't trade in existing positions unless it's stop loss
            if target != 0:
                continue
        log.info('{}: {}'.format(asset, target))
        order_target_percent(asset, target)
