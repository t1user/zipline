"""
Ingesting the full bundle is a lengthy process (about 2 hours on a decent machine). 
Settings in this file, allow to limit the number of contracts to be ingested
and select whether data should be downloaded or read from disk.

Full list of available symbols is here: 
https://s3.amazonaws.com/quandl-production-static/Ticker+CSV%27s/Futures/CMEGroup.csv

Single character symbols are preceded with '_' so 'C' (corn) becomes '_C'. That's necessary 
because zipline's continues_future requires at least 2 character symbols.

"""

# True: download data, False: read data from disk
# if run for the firsts time has to be True,
# because there no data saved on disk
DOWNLOAD = True

# list of symbols to ingest
# set to empty list to ingest all available symbols
contracts = ['ES', 'GC', 'JY', 'CL']
contracts = []


# This is a set of the most active contracts on CME
# Unconmment to use

"""
contracts = [
    # rates
    'ED', # eurodollars
    'TY', # 10y T-note
    'FV', # 5y T-note
    'TU', # 2y T-note
    'US', # U.S. Treasury bond
    'FF', # 30 day Fed funds
    'UL', # Ultra T-bond
    'SA', # 5y deliverable IRS
    'N1U',# 10y deliverable IRS
    'I3', # 30y deliverable IRS
    # equity
    'ES', # e-mini S&P 500
    'NQ', # e-mini Nasdaq 100
    'YM', # e-mini DowJones
    'MD', # e-mini S&P MidCap 400
    'NK', # Nikkei $5
    'XAF',# e-mini financial sector
    'XAK',# e-mini technology sector
    'XAP',# e-mini consumer staples sector
    'XAU',# e-mini utilities sector
    'XAY',# e-mini consumer discretionary sector
    # energy
    'CL', # WTI oil
    'NG', # natural gas
    'RB', # RBOB gasoline
    'HO', # NY Harbor ULSD
    'BZ', # Brent oil
    # fx
    'EC', # EUR/USD
    #'JY', # JPY/USD
    #'BP', # GBP/USD
    #'AD', # AUD/USD
    #'CD', # CAD/USD
    #'MP', # MXN/USD
    #'SF', # CHF/USD
    # agriculture
    '_C', # Corn
    '_S', # Soybeans
    '_W', # Chicago SRW Wheat
    'BO', # Soybean oil
    'SM', # Soybean meal
    'LC', # Live cattle
    'KW', # KC HRW wheat
    'LN', # Lean hogs
    # metals
    #'GC', # Gold
    'HG', # Copper
    'SI', # Silver
    #'PL', # Platinum
    #'PA', # Palladium
    ]
"""

# because of a misterious bug, symbols that have been commented out currently don't work, I'm working on it...
