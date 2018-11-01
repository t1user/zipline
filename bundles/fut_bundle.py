
import os
import sys
import numpy  as np
import pandas as pd
from six import iteritems
from io import BytesIO
import requests
from zipfile import ZipFile
import quandl
from logbook import Logger, StreamHandler, FileHandler
from zipline.data.bundles import core as bundles
from bundles.expiration_downloader import ExpirationDownloader


stream_handler = StreamHandler(sys.stdout, format_string=" | {record.message}", bubble=True)
log = Logger(__name__)
#stream_handler.push_application()


# filename for data downloaded from Quandl
QUANDL_ZIP_FILE = 'bundles/CME_price_data.zip'
# This file must contain: multiplier, tick_size, sector, sub-sector
# for every root symbol
META_FILE = 'bundles/meta.csv'


def csvdir_futures(tframes=None, csvdir=None):
    return CSVDIRFutures(tframes, csvdir).ingest


class CSVDIRFutures:
    """
    Wrapper class to call csvdir_bundle with provided
    list of time frames and a path to the csvdir directory
    """

    def __init__(self, tframes, csvdir):
        self.tframes = tframes
        self.csvdir = csvdir

    def ingest(self,
               environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir):

        futures_bundle(environ,
                       asset_db_writer,
                       minute_bar_writer,
                       daily_bar_writer,
                       adjustment_writer,
                       calendar,
                       start_session,
                       end_session,
                       cache,
                       show_progress,
                       output_dir,
                       self.tframes,
                       self.csvdir)

        
def get_meta_df():
    """
    Fetch metadata from csv file based on modified quandl supplied meta file.

    """
    return pd.read_csv(META_FILE, usecols=['root_symbol', 'name', 'exchange', 'multiplier',
                                         'tick_size', 'sector', 'sub_sector',])
    
def load_data_table(file,
                    index_col,
                    show_progress=False):
    """ Load data table from zip file provided by Quandl.
    """
    with ZipFile(file) as zip_file:
        file_names = zip_file.namelist()
        assert len(file_names) == 1, "Expected a single file from Quandl."
        prices = file_names.pop()
        with zip_file.open(prices) as table_file:
            if show_progress:
                log.info('Parsing raw data')
            df = pd.read_csv(
                table_file,
                error_bad_lines=False,
                header=None,
                parse_dates=[1],
                names=[
                    'symbol',
                    'date',
                    'open',
                    'high',
                    'low',
                    'close',
                    'change',
                    'settle',
                    'volume',
                    'open_interest',
                    'x', # placeholder to ensure parsing without errors
                    'y', # placeholder
                ],
            )
    # drop option codes which are mistakenly included in Quandl file
    df.drop(df[df['symbol'].str.len() > 8].index, inplace=True)
    # drop various indexes included in Quandl file
    df.drop(df[df['symbol'].str.contains('INDEX', regex=False) == True].index, inplace=True)
    # placeholders were only relevant for rows with option data, which are now removed
    del df['x']
    del df['y']
    return df

    
def fetch_data_table(show_progress, retries):
    """ Fetch CME data table from Quandl
    """
    for _ in range(retries):
        try:
            if show_progress:
                log.info('Downloading CME data')

            quandl.bulkdownload('CME', filename=QUANDL_ZIP_FILE)
                
            return load_data_table(
                file=QUANDL_ZIP_FILE,
                index_col=None,
                show_progress=show_progress,
                )

        except Exception:
            log.exception("Exception raised reading Quandl data. Retrying.")

    else:
        raise ValueError(
            "Failed to download Quandl data after %d attempts." % (retries)
        )


def fetch_quandl_specs_table(api_key, show_progress=False):
    """
    Return quandl spec file with a list of all available contracts.
    """
    if show_progress:
        log.info('Downloading metadata file from Quandl')

    r = requests.get('https://www.quandl.com/api/v3/databases/CME/metadata?api_key={}'
                     .format(api_key))
    r.raise_for_status()
    return pd.read_csv(BytesIO(r.content), compression='zip',
                       parse_dates=['from_date', 'to_date'])


def gen_asset_metadata(raw_data, quandl_specs, expiration, show_progress=False):
    if show_progress:
        log.info('Generating asset metadata')

    data = raw_data.groupby(
        by='symbol'
    ).agg(
        {'date': [np.min, np.max]}
    )

    data.reset_index(inplace=True)
    data.columns = data.columns.get_level_values(1)
    data.rename(columns={'': 'symbol', 'amin':'start_date', 'amax': 'end_date'},
                inplace=True)  
    data['first_traded'] = data['start_date']

    meta = get_meta_df()
    
    data['root_symbol'] = [s[:-5] for s in data.symbol.unique() ]
    names = quandl_specs['name']
    names.index = quandl_specs['code']
    data['asset_name'] = data.symbol.apply(lambda x: names.loc[x])

    # fix known error in quandl data
    data.loc[data[data['symbol'] == 'NQU1999'].index, 'asset_name'] = \
        'Nasdaq 100 Mini Futures, September 1999, NQU1999, CME'
    
    # include only contracts for which metadata is available
    data = data.merge(meta, on='root_symbol', how='inner')
    # precede single character roots with _, eg. C (corn) becomes _C
    data['root_symbol'] = data['root_symbol'].apply(lambda x: '_' + x if len(x) < 2 else x)

    # extract expiration dates
    d = data['end_date'].max() - pd.Timedelta(days=2)
    data['active'] = data['end_date'] >= d
    # expired contracts have data up to expiration date
    data['expiration_date'] = data[data['active']].symbol.apply(
        lambda x: expiration.get_date(x)).combine_first(
        data[~data['active']]['end_date'])
    data['auto_close_date'] = data['expiration_date'] - pd.Timedelta(days=2)
    data['notice_date'] = data['auto_close_date'] #- pd.Timedelta(days=1)
    data['expiration_year'] = data.symbol.apply(lambda x: x[-4:])

    return data.sort_values(by=['expiration_date', 'expiration_year', 'symbol']).reset_index(drop=True)


def parse_pricing_and_vol(data,
                          sessions,
                          symbol_map):
    for asset_id, symbol in iteritems(symbol_map):
        asset_data = data.xs(
            symbol,
            level=1
        ).reindex(
            sessions.tz_localize(None)
        ).fillna(0.0)
        yield asset_id, asset_data    

@bundles.register('futures')
def futures_bundle(environ,
                   asset_db_writer,
                   minute_bar_writer,
                   daily_bar_writer,
                   adjustment_writer,
                   calendar,
                   start_session,
                   end_session,
                   cache,
                   show_progress,
                   output_dir,
                   tframes=None,
                   csvdir=None):
    
    
    api_key = environ.get('QUANDL_API_KEY')
    if api_key is None:
        raise ValueError(
            "Please set your QUANDL_API_KEY environment variable and retry."
        )
    quandl.ApiConfig.api_key = api_key

    quandl_specs = fetch_quandl_specs_table(api_key, show_progress)
    expiration = ExpirationDownloader(quandl_specs)

    raw_data = fetch_data_table(
        show_progress,
        environ.get('QUANDL_DOWNLOAD_ATTEMPTS', 5)
    )
    asset_metadata = gen_asset_metadata(raw_data[['symbol', 'date']],
                                        quandl_specs,
                                        expiration,
                                        show_progress)

    # chop-off first two year digits from symbol, e.g. ESZ2018 becomes ESZ18 (actual CME symbol)
    #asset_metadata['symbol'] = asset_metadata['symbol'].apply(lambda x: x[:-4] + x[-2:])

    root_symbols = asset_metadata.root_symbol.unique()
    root_symbols = pd.DataFrame(root_symbols, columns = ['root_symbol'])
    root_symbols['root_symbol_id'] = root_symbols.index.values
    
    root_symbols['sector'] = [asset_metadata.loc[asset_metadata['root_symbol']==rs]['sector'].iloc[0] for rs in root_symbols.root_symbol.unique()]
    root_symbols['sub_sector'] = [asset_metadata.loc[asset_metadata['root_symbol']==rs]['sub_sector'].iloc[0] for rs in root_symbols.root_symbol.unique()]
    root_symbols['sector'] = root_symbols['sector'].str.cat(root_symbols['sub_sector'], sep='/')


    root_symbols['exchange'] = [asset_metadata.loc[asset_metadata['root_symbol']==rs]['exchange'].iloc[0] for rs in root_symbols.root_symbol.unique() ]
    root_symbols['description'] = [asset_metadata.loc[asset_metadata['root_symbol']==rs]['name'].iloc[0] for rs in root_symbols.root_symbol.unique() ]

    # create empty SQLite tables to prevent lookup errors in algorithms
    divs_splits = {'divs': pd.DataFrame(columns=['sid', 'amount', 'ex_date', 'record_date',
                                              'declared_date', 'pay_date']),
                   'splits': pd.DataFrame(columns=['sid', 'ratio', 'effective_date'])}
    adjustment_writer.write(splits=divs_splits['splits'], dividends=divs_splits['divs'])


    asset_db_writer.write(futures=asset_metadata, root_symbols=root_symbols)

    symbol_map = asset_metadata.symbol
    sessions = calendar.sessions_in_range(start_session, end_session)
    raw_data.set_index(['date', 'symbol'], inplace=True)
    daily_bar_writer.write(
        parse_pricing_and_vol(
            raw_data,
            sessions,
            symbol_map
        ),
        show_progress=show_progress
    )
    




