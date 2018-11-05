Custom bundle for ingesting Quandl CME futures data (https://www.quandl.com/data/CME-Chicago-Mercantile-Exchange-Futures-Data) into zipline (https://www.zipline.io/)

Note:
Some zipline functions don't work with single character futures symbols so they have been preceded with underscore, so C (corn) becomes _C


Requirements:
-------------

1. set environment variable QUANDL_API_KEY

2. install packages:
- zipline (obviously -:)
- quandl
- requests-html
- xlrd

3. in
~/.zipline/extension.py
add:
from bundles.extension import register

4. in
.../site-packages/zipline/utils/run_algo.py
add line 155:
future_daily_reader=bundle_data.equity_daily_bar_reader,

5. add to  PYTHONPATH the directory to which you cloned this repo
(eg. in my case on windows computer: set PYTHONPATH %userprofile%/zipline)


Usage:
-------------

from CLI:

zipline ingest -b futures


The whole ingestion process takes almost 2 hours (on a fairly decent machine). 
