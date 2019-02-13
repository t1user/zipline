# WORK IN PROGRESS

import pandas as pd
import numpy as np
import talib
from zipline.api import (continuous_future, get_open_orders,
                         order_target_percent, set_slippage,
                         set_commission, get_datetime, record
                         )
from contracts import contracts
from strategy_1_a import (get_data, get_entries, get_rolls,
                          get_stops, process_signals, trade)
from strategy_1_a import initialize as init

# FAST_MA = 50
#SLOW_MA = 100
# BREAKOUT = 50  # breakout beyond x days max/min
# STOP = 2  # stop after x atr
RISK = .3  # % of capital daily risk per position
MAX_EXP = 50  # max exposure per position as percent of equity
REBALANCE = True
VOL_DAYS = 20
TARGET_VOL = .12


def initialize(context):
    init(context)
    context.counter = 0
    context.model_portfolio = pd.Series()


def handle_data(context, data):

    get_data(context, data)
    # generate and process trading signals
    entries = get_entries(context)
    rolls = get_rolls(context)
    stops = get_stops(context)
    signals = pd.concat([entries, rolls, stops])
    positions, stops = process_signals(context, signals)

    # optimize portfolio and trade
    if not signals.empty:
        portfolio = optimize_portfolio(context, positions)
        trade(context, portfolio, stops)


def optimize_portfolio(context, target_positions):
    context.rebalance = False
    weights = RISK/100 * context.last_price/context.atr
    # correct desired weights by correlation ranking
    correlations = get_correlations(context)
    weights *= correlations
    # dictionary to translate between ContinuousFuture objects and root_symbols
    target_contracts = {contract.root_symbol: contract
                        for contract in target_positions.index}
    # select relevant contracts and
    # translate index from ContinuousFuture to root_symbol
    weights = weights[list(target_contracts.keys())]
    weights.index = weights.index.map(lambda x: target_contracts[x])
    # MAX_EXP is a limiter on absolute position size
    target_positions *= weights.clip(upper=MAX_EXP)
    target_positions *= get_vol(context, target_positions)
    record(atr=context.atr, target=target_positions, correlations=correlations)
    context.model_portfolio.update(target_positions)
    context.counter += 1
    if context.counter == 6:
        context.rebalance = True
        context.counter = 0
        return context.model_portfolio
    else:
        return target_positions


def get_correlations(context):
    returns = np.log(context.prices.pct_change()+1)[1:]
    corr = returns.corr()
    count = corr.apply(lambda x: x[x < 0.2].count())
    buckets = pd.cut(count, 3, labels=[0.5, 1, 1.5],).sort_values()
    return buckets


def get_vol(context, target_positions):
    returns = np.log(context.prices.pct_change()+1)[-VOL_DAYS:]
    target_positions = target_positions.copy()
    target_positions.index = target_positions.index.map(
        lambda x: x.root_symbol)
    returns = returns[list(target_positions.index)]
    std = returns.dot(target_positions).std() * np.sqrt(252)
    if abs(TARGET_VOL/std - 1) > .1:
        return TARGET_VOL / std
    else:
        return 1
