# WORK IN PROGRESS

import pandas as pd
import numpy as np
import talib
from zipline.api import (order, record, symbol, continuous_future,
                         future_symbol, get_open_orders, order_target_percent,
                         set_slippage, set_commission, get_datetime,
                         schedule_function, date_rules, time_rules)
from zipline.finance.slippage import FixedSlippage, SlippageModel
from zipline.finance.commission import PerTrade
from contracts import contracts
import scipy.linalg as lg
import pdb

FAST_MA = 25
SLOW_MA = 100
BREAKOUT = 50  # breakout beyond x days max/min
STOP = 2  # stop after x atr
RISK = .005  # fraction of portfolio in daily portfolio std
POS_LIMIT = .002  # daily position risk limit as fraction of portfolio in position std


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

    context.slow_ma = hist['price'].apply(lambda x: talib.EMA(
        x.values, timeperiod=SLOW_MA)[-1])
    context.fast_ma = hist['price'].apply(lambda x: talib.EMA(
        x.values, timeperiod=FAST_MA)[-1])
    context.atr = hist.apply(lambda x: talib.ATR(
        x['high'].fillna(x['price']).values,
        x['low'].fillna(x['price']).values,
        x['price'].values,
        timeperiod=SLOW_MA)[-1],
        axis=(1, 0))

    # std = hist.pct_change().std()

    # make variables available globally
    context.prices = hist['price']
    context.last_price = context.prices.fillna(method='ffill').iloc[-1]

    # Series for translations from ContinuousFuture and current Future objects
    context.translate = data.current(
        context.prices.columns, 'contract')
    # Series for translations from root_symbol to current Future objects
    context.translate_root = pd.Series({contract.root_symbol: contract
                                        for contract in context.translate.values})

    # reindex to use root_symbol instead of ContinuousFuture
    reindex(context.atr, context.last_price, context.slow_ma,
            context.fast_ma)
    context.prices.columns = context.prices.columns.map(
        lambda x: x.root_symbol)

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


def get_entries(context):
    """
    Generate position entry signals.
    args:
    DataFrame with index: dates, columns: continuous_future objects
    returns:
    Series with index: continuous_future, columns: 1 or -1 for long or short signal
    """
    # breakout above is a buy signal
    upper = context.prices[-BREAKOUT-1:-2].max(axis=0)
    # breakout below is a sell signal
    lower = context.prices[-BREAKOUT-1:-2].min(axis=0)

    longs = ((context.last_price > upper) & (
        context.fast_ma > context.slow_ma)) * 1
    shorts = ((context.last_price < lower) & (
        context.fast_ma < context.slow_ma)) * -1
    signals = longs + shorts
    signals = signals[signals != 0].dropna()
    # convert index from continues_future to future object
    signals.index = signals.index.map(lambda x: context.translate_root.get(x))

    return signals


def get_rolls(context):
    """
    Rollover expiring contracts for existing positions.
    """
    signals = pd.Series()
    for cont, pos in context.portfolio.positions.items():
        if cont not in context.translate.values:
            sign = int(
                np.sign(context.portfolio.current_portfolio_weights[cont]))
            # close existing contract
            signals[cont] = 0
            root = cont.root_symbol
            current = context.translate.get(continuous_future(root))
            position = context.portfolio.positions[cont]
            # open current contract only if unrealised PnL positive
            if (position.last_sale_price
                    - position.cost_basis) * position.amount > 0:
                if current:  # contract could've been delisted
                    signals[current] = 1 * sign
    return signals


def get_stops(context):
    """
    Rules for closing out positions regardless of whether
    they're profitable or loss making.
    """
    signals = pd.Series()
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
        # get out if contract expired
        if not context.atr.get(contract.root_symbol):
            continue
        # calculate stop loss level
        if position.amount > 0:
            stop_price = context.min_max[contract][1] - \
                context.atr.get(contract.root_symbol).item() * STOP
            if context.last_price.get(contract.root_symbol) <= stop_price:
                signals[contract] = 0
                del context.min_max[contract]
        if position.amount < 0:
            stop_price = context.min_max[contract][0] + \
                context.atr.get(contract.root_symbol).item() * STOP
            if context.last_price.get(contract.root_symbol) >= stop_price:
                signals[contract] = 0
                del context.min_max[contract]
    return signals


def process_signals(context, signals):
    """
    Combine new position signals with existing portfolio.
    Returns a tuple: (target_positions, stops), where:
    target_positions: pandas Series with index: Future objects, values: -1, 1, or 0 
    for desired position direction.
    stops: pandas Series with index: Future object, values: 0 for positions to be closed
    """
    # create a Series with desired positions and trade direction
    # existing positions
    target_positions = pd.Series({contract: np.sign(position.amount)
                                  for contract, position in context.portfolio.positions.items()})
    # remove positions to be closed
    target_positions.drop(signals[signals == 0].index, inplace=True)
    # append new positions to be opened
    target_positions = target_positions.append(signals[signals != 0])
    # remove duplicates (where signal is generated for contract which is already open)
    target_positions = target_positions[~target_positions.index.duplicated()]

    # generate list of positions to be closed
    stops = signals[signals == 0]
    return target_positions, stops


def optimize_portfolio(context, target_positions):

    # get average returns for target list of assets
    prices = context.prices.copy()
    positions_roots = [x.root_symbol for x in target_positions.index]
    prices = prices[positions_roots]
    returns_df = prices.pct_change()[1:]
    returns = returns_df.mean().values
    #returns = returns_df.mean().abs().values
    returns = np.multiply(returns, target_positions.values)
    # returns = returns_df.mean().abs().values

    # convert to numpy and optimize
    # returns1 = np.mean(returns_df.values, axis=0)
    # returns = np.abs(returns)

    covariance_matrix = np.asmatrix(np.cov(returns_df.T.values))

    optimized_weights = max_sharpe(covariance_matrix, returns)

    # match weights with respective assets
    optimized = pd.Series(data=optimized_weights, index=target_positions.index)

    # calculate optimized portfolio std
    std = np.sqrt(optimized_weights.T.dot(
        covariance_matrix).dot(optimized_weights))
    # scale to required risk level
    risk_factor = RISK/std
    optimized = optimized * risk_factor.item()

    # max_weights = POS_LIMIT/100 * context.last_price/std
    # returns for every

    return optimized


def max_sharpe(cov, returns):
    """
    Find weights of a portfolio with maximum sharpe.
    Args: 
    cov: covariance matriix (numpy array)
    returns: average returns (numpy array)
    Returns:
    numpy array of weights
    """
    n = len(returns)
    onesT = np.ones((n, 1)).T
    covis = np.linalg.inv(cov)  # invert cov matrix
    p1 = np.dot(onesT, covis)
    p1 = np.dot(p1, returns)
    p2 = np.dot(covis, returns)
    w = p2 / p1  # divide the two products to find weights
    return np.ravel(w) / np.sum(np.abs(w))  # normalize weights to one


def trade(context, positions, stops):
    """
    Execute trades.
    """
    trades = positions.append(stops)
    for asset, target in trades.items():
        order_target_percent(asset, target)


def reindex(*args):
    """
    Reindex in place passed dataframes from ContinuousFuture to root_symbol.
    """
    for dataframe in args:
        dataframe.index = dataframe.index.map(lambda x: x.root_symbol)
