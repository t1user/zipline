import pandas as pd
from pyfolio.txn import map_transaction



def format_asset(asset):
    """
    If zipline asset objects are used, we want to print them out prettily
    within the tear sheet. This function should only be applied directly
    before displaying.
    """

    try:
        import zipline.assets
    except ImportError:
        return asset

    if isinstance(asset, zipline.assets.Asset):
        return asset.symbol
    else:
        return asset


def extract_pos(positions, cash):
    """
    Extract position values from backtest object as returned by
    get_backtest() on the Quantopian research platform.

    Parameters
    ----------
    positions : pd.DataFrame
        timeseries containing one row per symbol (and potentially
        duplicate datetime indices) and columns for amount and
        last_sale_price.
    cash : pd.Series
        timeseries containing cash in the portfolio.

    Returns
    -------
    pd.DataFrame
        Daily net position values.
         - See full explanation in tears.create_full_tear_sheet.
    """

    positions = positions.copy()
    positions['values'] = positions.amount * positions.last_sale_price * positions.sid.map(lambda x: x.multiplier)
    cash.name = 'cash'

    values = positions.reset_index().pivot_table(index='index',
                                                 columns='sid',
                                                 values='values')

    values = values.join(cash).fillna(0)

    # NOTE: Set name of DataFrame.columns to sid, to match the behavior
    # of DataFrame.join in earlier versions of pandas.
    values.columns.name = 'sid'

    return values


def make_transaction_frame(transactions):
    """
    Formats a transaction DataFrame.

    Parameters
    ----------
    transactions : pd.DataFrame
        Contains improperly formatted transactional data.

    Returns
    -------
    df : pd.DataFrame
        Daily transaction volume and dollar ammount.
         - See full explanation in tears.create_full_tear_sheet.
    """

    transaction_list = []
    for dt in transactions.index:
        txns = transactions.loc[dt]
        if len(txns) == 0:
            continue

        for txn in txns:
            txn = map_transaction(txn)
            transaction_list.append(txn)
    df = pd.DataFrame(sorted(transaction_list, key=lambda x: x['dt']))
    df['txn_dollars'] = -df['amount'] * df['price'] * df['sid'].apply(lambda x: x.multiplier)

    df.index = list(map(pd.Timestamp, df.dt.values))
    return df

  
def extract_returns(backtest):
    """
    Extract returns, positions, transactions and leverage from the
    backtest data structure returned by zipline.TradingAlgorithm.run().

    The returned data structures are in a format compatible with the
    rest of pyfolio and can be directly passed to
    e.g. tears.create_full_tear_sheet().

    Parameters
    ----------
    backtest : pd.DataFrame
        DataFrame returned by zipline.TradingAlgorithm.run()

    Returns
    -------
    returns : pd.Series
        Daily returns of strategy.
         - See full explanation in tears.create_full_tear_sheet.
    positions : pd.DataFrame
        Daily net position values.
         - See full explanation in tears.create_full_tear_sheet.
    transactions : pd.DataFrame
        Prices and amounts of executed trades. One row per trade.
         - See full explanation in tears.create_full_tear_sheet.


    Example (on the Quantopian research platform)
    ---------------------------------------------
    >>> backtest = my_algo.run()
    >>> returns, positions, transactions =
    >>>     pyfolio.utils.extract_rets_pos_txn_from_zipline(backtest)
    >>> pyfolio.tears.create_full_tear_sheet(returns,
    >>>     positions, transactions)
    """
 
    backtest.index = backtest.index.normalize()
    if backtest.index.tzinfo is None:
        backtest.index = backtest.index.tz_localize('UTC')
    returns = backtest.returns
    raw_positions = []
    for dt, pos_row in backtest.positions.iteritems():
        df = pd.DataFrame(pos_row)
        df.index = [dt] * len(df)
        raw_positions.append(df)
    if not raw_positions:
        raise ValueError("The backtest does not have any positions.")
    positions = pd.concat(raw_positions)
    positions = extract_pos(positions, backtest.ending_cash)
    transactions = make_transaction_frame(backtest.transactions)
    if transactions.index.tzinfo is None:
        transactions.index = transactions.index.tz_localize('utc')

    return returns, positions, transactions
