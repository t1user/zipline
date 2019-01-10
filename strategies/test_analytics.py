import pandas as pd
import pyfolio as pf
from extract_returns import extract_returns

results = pd.read_pickle('results_1a_2016-2017.pickle')
returns, positions, transactions = extract_returns(results)

pos = list(positions.columns)
pos.remove('cash')
sectors = pd.read_csv('../bundles/meta.csv',
                      usecols=['root_symbol', 'sector', 'sub_sector'],
                      index_col=['root_symbol'])
sectors['sector'] = sectors['sector'].str.cat(sectors['sub_sector'], sep='/')
del sectors['sub_sector']
sectors.index = sectors.index.map(lambda x: x if len(x) > 1 else '_' + x)
sectors = sectors.T.to_dict(orient='records')[0]
sector_map = {p: sectors[p.root_symbol] for p in pos}

out_of_sample = results.index[-21]
transactions_mod = transactions.copy()
transactions_mod.price = transactions.sid.apply(
    lambda x: x.multiplier) * transactions.price
benchmark = (results['benchmark_period_return'] + 1).pct_change()[1:]
benchmark.index = benchmark.index.normalize()

pf.create_full_tear_sheet(returns,
                          positions=positions,
                          transactions=transactions_mod,
                          live_start_date=out_of_sample,
                          round_trips=True, benchmark_rets=benchmark,
                          sector_mappings=sector_map, hide_positions=True)
