import os
import numpy  as np
import pandas as pd
from . import core as bundles
import sys
from logbook import Logger, StreamHandler, FileHandler
from tqdm import tqdm
from six import iteritems


handler = StreamHandler(sys.stdout, format_string=" | {record.message}", bubble=True)
log = Logger(__name__)
stream_handler.push_application()


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
    Fetches metadata from csv file downloaded from csi.
    File commodityfactsheet.csv must be in the working directory.
    """
    df = pd.read_csv('commodityfactsheet.csv', usecols=['SymbolCommercial', 'Name', 'Exchange', 'FullPointValue', 'MinimumTick'])
    df.rename(columns={'SymbolCommercial': 'root_symbol', 
                       'Name': 'name', 
                       'FullPointValue': 'multiplier', 
                       'MinimumTick': 'tick_size'}, inplace=True)
    df['tick_size'] = df['tick_size'] / 100
    return df

def get_symbol(filename):
    # chop-off .csv extension
    return filename.split('.')[0]


def load_data(path='data'):
    filelist = [s for s in os.listdir(path)]  
    #layout = ['Date', 'Open','High','Low','Last','Change', 'Settle', 'Volume','Previous Day Open Interest']
    df_list = []
    for file in tqdm(filelist):
        df = pd.read_csv(os.path.join(path, file), parse_dates=[0])
        df['symbol'] = get_symbol(file)
        df_list.append(df)
    big_df = pd.concat(df_list)
    big_df.columns = map(str.lower, big_df.columns)
    return big_df
    

def gen_asset_metadata(raw_data, show_progress):
    if show_progress:
        log.info('Generating asset metadata.')

    data = raw_data.groupby(
        by='symbol'
    ).agg(
        {'date': [np.min, np.max]}
    )

    data.columns = data.columns.get_level_values(1)
    data.rename(columns={'amin':'start_date', 'amax': 'end_date'}, inplace=True)  
    data['first_traded'] = data['start_date']

    meta = get_meta_df()
    
    data['root_symbol'] = [s[:-5] for s in data.symbol.unique() ] 
    data = data.merge(meta, on='root_symbol')
    
    data['auto_close_date'] = data['end_date'] #+ pd.Timedelta(days=1)
    data['notice_date'] = data['auto_close_date']

    return data
    
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

    
    raw_data = load_data('-----')
    asset_metadata = gen_asset_metadata(raw_data, False)
    root_symbols = asset_metadata.root_symbol.unique()
    root_symbols = pd.DataFrame(root_symbols, columns = ['root_symbol'])
    root_symbols['root_symbol_id'] = root_symbols.index.values
    
    root_symbols['sector'] = [asset_metadata.loc[asset_metadata['root_symbol']==rs]['sector'].iloc[0] for rs in root_symbols.root_symbol.unique() ]
    root_symbols['exchange'] = [asset_metadata.loc[asset_metadata['root_symbol']==rs]['exchange'].iloc[0] for rs in root_symbols.root_symbol.unique() ]
    root_symbols['description'] = [asset_metadata.loc[asset_metadata['root_symbol']==rs]['asset_name'].iloc[0] for rs in root_symbols.root_symbol.unique() ]
    
    
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

