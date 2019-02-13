import pickle
import pandas as pd
import numpy as np
import talib
from collections import OrderedDict
from zipline.api import (continuous_future, get_open_orders,
                         order_target_percent, set_slippage,
                         set_commission, get_datetime, record
                         )
from zipline.finance.slippage import SlippageModel
from zipline.finance.commission import PerTrade
from zipline import run_algorithm
from contracts import contracts


FAST_MA = 50  # 50
SLOW_MA = 300  # 200
BREAKOUT = 50  # breakout beyond x days max/min
STOP = 3  # 3 stop after x atr
RISK = .4  # % of capital daily risk per position
MAX_EXP = 50  # max exposure per position as percent of equity
VOL_DAYS = 60  # number of days to calculate realised volatility
TARGET_VOL = .12  # target volatility
REB_FREQUENCY = 5  # rebalance frequency in days

# ON/OFF switches for portfolio optimization methods
CORR_1 = True
CORR_2 = False  # works only if CORR_1 is False
CAP_EXP = True  # cut exposure per pos to MAX_EXP
VOL_TARGET = True  # target portf vol at TARGET_VOL
REBALANCE = True  # whether existing pos. should be adjusted for changes in port vol


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
    # auxiliary variables selectively used in various portfolio optimization methods
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


def reindex(*args):
    """
    Reindex in place passed dataframes from ContinuousFuture to root_symbol.
    """
    for dataframe in args:
        dataframe.index = dataframe.index.map(lambda x: x.root_symbol)


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


def optimize_portfolio(context, target_positions):
    context.rebalance = False
    weights = RISK/100 * context.last_price/context.atr
    # correct desired weights by correlation ranking
    if CORR_1:
        correlations = get_correlations_1(context)
    elif CORR_2:
        correlations = get_correlations_2(context)
    else:
        correlations = 1
    weights *= correlations
    # dictionary to translate between ContinuousFuture objects and root_symbols
    target_contracts = {contract.root_symbol: contract
                        for contract in target_positions.index}
    # select relevant contracts and
    # translate index from ContinuousFuture to root_symbol
    weights = weights[list(target_contracts.keys())]
    weights.index = weights.index.map(lambda x: target_contracts[x])
    # MAX_EXP is a limiter on absolute position size
    if CAP_EXP:
        weights = weights.clip(upper=MAX_EXP)
    target_positions *= weights
    if VOL_TARGET:
        target_positions *= get_vol(context, target_positions)
    if REBALANCE:
        rebalance_switch(context)
    # record(atr=context.atr, target=target_positions, correlations=correlations)
    context.target_portfolio.update(target_positions)
    return target_positions


def get_vol(context, target_positions):
    """
    Calculate realized portfolio volatility. Return adjustment factor to get from 
    realised to target vol.
    """
    returns = np.log(context.prices.pct_change()+1)[-VOL_DAYS:]
    target_positions = target_positions.copy()
    target_positions.index = target_positions.index.map(
        lambda x: x.root_symbol)
    current_alloc = get_current_allocations(context)

    target_positions.update(current_alloc)

    returns = returns[list(target_positions.index)]
    std = returns.dot(target_positions).std() * np.sqrt(252)
    if abs(TARGET_VOL/std - 1) > .1:
        return TARGET_VOL / std
    else:
        return 1


def get_current_allocations(context):
    portfolio = pd.Series(context.portfolio.positions)
    portfolio = portfolio.map(
        lambda x: x.amount * x.last_sale_price * x.asset.multiplier)
    portfolio /= context.portfolio.portfolio_value
    portfolio.index = portfolio.index.map(lambda x: x.root_symbol)
    # duplicates may occassionally happen when roll done on the last day
    # of life of the front contract
    portfolio = portfolio[~portfolio.index.duplicated()]
    return portfolio


def rebalance_switch(context):
    context.counter += 1
    if context.counter == REB_FREQUENCY:
        context.rebalance = True
        context.counter = 0


def get_correlations_1(context):
    """
    Pairwise correlation of all assets. Rank by number of correlations lower than .2. 
    Split into three buckets.
    """
    returns = np.log(context.prices.pct_change()+1)[1:]
    corr = returns.corr()
    count = corr.apply(lambda x: x[x < 0.2].count())
    buckets = pd.cut(count, 3, labels=[0.5, 1, 1.5],).sort_values()
    return buckets


def get_correlations_2(context):
    """
    Correlation of every asset vs. equally weighted portfolio of all other assets.
    Rank results and split assets into three buckets.
    """
    returns = np.log(context.prices.pct_change()+1)[1:]
    corrs = {}
    for symbol in returns.columns:
        corrs[symbol] = returns[symbol].corr(
            returns.drop(symbol, axis=1).apply(
                lambda x: np.average(x), axis=1))
    c = pd.Series(corrs)
    buckets = pd.qcut(c, 3, labels=[1.5, 1, .5]).sort_values()
    return buckets


def test_strategy(params_dict, file_name):
    results = OrderedDict()
    for key, value in params_dict.items():
        global CORR_1, CORR_2, CAP_EXP, VOL_TARGET, REBALANCE
        CORR_1, CORR_2, CAP_EXP, VOL_TARGET, REBALANCE = value

        test = run_algorithm(start=pd.Timestamp('2013-01-01', tz='utc'),
                             end=pd.Timestamp('2018-10-31', tz='utc'),
                             initialize=initialize,
                             handle_data=handle_data,
                             bundle='futures',
                             capital_base=1e+6,
                             )

        results[key] = test

    with open('results/{}.pickle'.format(file_name), 'wb') as file:
        pickle.dump(results, file)


if __name__ == '__main__':

    """
    singles = OrderedDict(
        [('no_opt', (False, False, False, False, False)),
         ('corr_1', (True, False, False, False, False)),
         ('corr_2', (False, True, False, False, False)),
         ('cap_exp', (False, False, True, False, False)),
         ('vol_target', (False, False, False, True, False)),
         ])
    combined = OrderedDict(
        [('corr_cap', (True, False, True, False, False)),
         ('corr_cap_vol', (True, False, True, True, False)),
         ('corr_cap_vol_reb', (True, False, True, True, True)),
         ('cap_vol_reb', (False, False, True, True, True)),
         ])

    #test_strategy(singles, 'single_parameter_results')
    #test_strategy(combined, 'combined_parameter_results')

    fast_ma = [10, 25, 50, 75]
    slow_ma = [100, 200, 300, 400]
    breakout = [25, 50, 75, 100]
    stop = [1, 2, 3, 4]
    risk = [.1, .2, .3, .4]
    max_exp = [25, 50, 100]
    vol_days = [20, 60, 125]
    target_vol = [.08, .10, .12, .14]
    reb_frequency = [0, 5, 21]

    file_name = 'exp_days'

    results = {}
    for exp in max_exp:
        for day in vol_days:
            MAX_EXP, VOL_DAYS = exp, day
            test = run_algorithm(start=pd.Timestamp('2013-01-01', tz='utc'),
                                 end=pd.Timestamp('2018-10-31', tz='utc'),
                                 initialize=initialize,
                                 handle_data=handle_data,
                                 bundle='futures',
                                 capital_base=1e+6,
                                 )
            key = 'exp{}_days{}'.format(exp, day)
            results[key] = test

            with open('results/{}.pickle'.format(file_name), 'wb') as file:
                pickle.dump(results, file)

    """
    file_name = 'maximize_return_2007-2012'
    test = run_algorithm(start=pd.Timestamp('2007-01-01', tz='utc'),
                         end=pd.Timestamp('2012-12-31', tz='utc'),
                         initialize=initialize,
                         handle_data=handle_data,
                         bundle='futures',
                         capital_base=1e+6,
                         )

    with open('results/{}.pickle'.format(file_name), 'wb') as file:
        pickle.dump(test, file)
