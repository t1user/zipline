Custom bundle for ingesting Quandl CME futures data (https://www.quandl.com/data/CME-Chicago-Mercantile-Exchange-Futures-Data) into zipline (https://www.zipline.io/)

settings.py in bundles directory allows to select contracts for which data is ingested and whether data should be downloaded or read from disk.

Unlike on Quandl, years in futures symbols are encoded as two digits (in line with CME and Quantopian), so symbol for December 2018 S&P500 contract is ESZ18 (rather than ESZ2018 as on Quandl).

Some zipline functions don't work with single character futures root symbols so they have been preceded with underscore, eg. C (corn) becomes _C


Requirements:
-------------

1. set environment variable QUANDL_API_KEY

2. install packages:
- zipline 1.3.0
- quandl
- requests-html
- xlrd

3. in
~/.zipline/extension.py
add:
from bundles.extension import *

4. in
.../site-packages/zipline/utils/run_algo.py
add line 169:
future_daily_reader=bundle_data.equity_daily_bar_reader,
(additional argument passed to DataPortal class)

5. add to PYTHONPATH directory where this code resides
(i.e. parent directory to bundles/)


Usage:
-------------

from CLI:

zipline ingest -b futures



