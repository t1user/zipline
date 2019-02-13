# WORK IN PROGRESS

import pandas as pd
import numpy as np
import talib
from zipline.api import (continuous_future, get_open_orders,
                         order_target_percent, set_slippage,
                         set_commission, get_datetime, record
                         )
from zipline.finance.slippage import SlippageModel
from zipline.finance.commission import PerTrade
from contracts import contracts


FAST_MA = 50
SLOW_MA = 200
BREAKOUT = 50  # breakout beyond x days max/min
STOP = 3  # stop after x atr
RISK = .3  # % of capital daily risk per position
REBALANCE = False


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
    # auxiliary variables selectively used by various strategies
    context.rebalance = False
    context.counter = 0
    context.target_portfolio = pd.Series()


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


def get_data(context, data):
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
        axis=(1, 0)).fillna(method='ffill')

    # std = hist.pct_change().std()

    # make variables available globally
    context.prices = hist['price']
    context.last_price = context.prices.fillna(method='ffill').iloc[-1]

    # Series for translations from ContinuousFuture to current Future objects
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
    # remove potential duplicates (roll + stop-out on the same day)
    stops = stops[~stops.index.duplicated()]
    return target_positions, stops


def optimize_portfolio(context, target_positions):

    weights = RISK/100 * context.last_price/context.atr
    # dictionary to translate between ContinuousFuture objects and root_symbols
    target_contracts = {contract.root_symbol: contract
                        for contract in target_positions.index}
    # select relevant contracts and
    # translate index from ContinuousFuture to root_symbol
    weights = weights[list(target_contracts.keys())]
    weights.index = weights.index.map(lambda x: target_contracts[x])

    target_positions = target_positions * weights
    record(atr=context.atr, target=target_positions)
    return target_positions


def trade(context, positions, stops):
    """
    Execute trades.
    """
    existing_positions = list(context.portfolio.positions.keys())
    if not context.rebalance:
        trades = context.target_portfolio
    trades = positions.append(stops)
    orders = get_open_orders()
    for asset, target in trades.items():

        if not context.rebalance:
            if asset in existing_positions:
                # don't trade in existing positions unless it's stop loss
                # i.e. don't adjust position size for changes in volatility
                if target != 0:
                    continue

        if asset not in orders:
            # don't issue new orders if existing orders haven't been filled
            order_target_percent(asset, target)


def reindex(*args):
    """
    Reindex in place passed dataframes from ContinuousFuture to root_symbol.
    """
    for dataframe in args:
        dataframe.index = dataframe.index.map(lambda x: x.root_symbol)
