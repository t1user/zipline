import os
import numpy  as np
import pandas as pd
from zipline.data.bundles import core as bundles
from zipline.assets.futures import CME_CODE_TO_MONTH
import sys
from logbook import Logger, StreamHandler, FileHandler
from tqdm import tqdm
from six import iteritems
import calendar as cal
#from bundles.symbol_mapper import Mapper


stream_handler = StreamHandler(sys.stdout, format_string=" | {record.message}", bubble=True)
log = Logger(__name__)
stream_handler.push_application()
#mapper = Mapper()

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
                       #'Name': 'name',
                       'Exchange': 'exchange',
                       'FullPointValue': 'multiplier', 
                       'MinimumTick': 'tick_size'}, inplace=True)
    df['tick_size'] = df['tick_size'] / 100
    # rows with duplicated symbols are useless for further processing
    df.drop(df[df.root_symbol.duplicated()].index, inplace=True)
    # get proper contract names from quandl CME meta file
    CME_df = pd.read_csv('CMEGroup.csv', usecols=[0, 2], header=None)
    CME_df.columns=['root_symbol', 'name']
    data = CME_df.merge(df, on='root_symbol', how='left')
    data.drop('Name', axis=1, inplace=True)
    return data

def get_symbol(filename):
    # chop-off .csv extension
    return filename.split('.')[0]

def get_expiration(symbol):
    """
    work in progress
    returns 3 Friday of the expiration month
    """
    year = int(symbol[-4:])
    month = int(CME_CODE_TO_MONTH[symbol[-5]])
    c = cal.Calendar(firstweekday=cal.SATURDAY)
    day = c.monthdatescalendar(year, month)[2][-1]
    return pd.to_datetime('{}'.format(day))

def load_data(path='data'):
    filelist = [s for s in os.listdir(path)]  
    #layout = ['Date', 'Open','High','Low','Last','Change', 'Settle', 'Volume','Previous Day Open Interest']
    df_list = []
    for file in tqdm(filelist):
        df = pd.read_csv(os.path.join(path, file), parse_dates=[0])
        
        # quandl data has 3 different names for this column
        oi = df.columns[8]
        df.rename(columns={oi: 'open interest'}, inplace=True)
        
        df['symbol'] = get_symbol(file)

        # assert that required columns exist in the file
        required_columns = set(['Date', 'Open', 'High', 'Low', 'Last',
                                'Change', 'Settle', 'Volume',
                                'open interest', 'symbol'])
        existing_columns = set(df.columns)
        try:
            assert required_columns.issubset(existing_columns)
        except AssertionError:
            log.error('Missing columns in file: {}'.format(file))
        
        df['close'] = df['Settle']
        df_list.append(df)
    big_df = pd.concat(df_list)
    big_df.columns = map(str.lower, big_df.columns)
    return big_df

def gen_asset_metadata(raw_data, show_progress=True):
    if show_progress:
        log.info('Generating asset metadata.')

    data = raw_data.groupby(
        by='symbol'
    ).agg(
        {'date': [np.min, np.max]}
    )

    data.reset_index(inplace=True)
    data.columns = data.columns.get_level_values(1)
    data.rename(columns={'': 'symbol', 'amin':'start_date', 'amax': 'end_date'}, inplace=True)  
    data['first_traded'] = data['start_date']

    meta = get_meta_df()
    
    data['root_symbol'] = [s[:-5] for s in data.symbol.unique() ] 
    data = data.merge(meta, on='root_symbol', how='left')
    #data['root_symbol'].apply(lambda x: mapper.filter(x))

    # temporary workaround for expiration dates
    d = data['end_date'].max() - pd.Timedelta(days=2)
    data['active'] = data['end_date'] >= d
    data['expiration_date'] = data[data['active']].symbol.apply(lambda x: get_expiration(x)).combine_first(
        data[~data['active']]['end_date'])
    
    data['auto_close_date'] = data['expiration_date'] - pd.Timedelta(days=2)
    data['notice_date'] = data['auto_close_date'] #- pd.Timedelta(days=1)
    return data.sort_values(by='expiration_date').reset_index(drop=True)
    
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

    
    raw_data = load_data()
    asset_metadata = gen_asset_metadata(raw_data)
    root_symbols = asset_metadata.root_symbol.unique()
    root_symbols = pd.DataFrame(root_symbols, columns = ['root_symbol'])
    root_symbols['root_symbol_id'] = root_symbols.index.values
    
    root_symbols['sector'] = 'placeholder'
    #[asset_metadata.loc[asset_metadata['root_symbol']==rs]['sector'].iloc[0] for rs in root_symbols.root_symbol.unique() ]
    root_symbols['exchange'] = [asset_metadata.loc[asset_metadata['root_symbol']==rs]['exchange'].iloc[0] for rs in root_symbols.root_symbol.unique() ]
    root_symbols['description'] = [asset_metadata.loc[asset_metadata['root_symbol']==rs]['name'].iloc[0] for rs in root_symbols.root_symbol.unique() ]
    
    
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
    
    #mapper.save()



