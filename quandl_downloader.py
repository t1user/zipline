import numpy as np
import pandas as pd
import quandl
import requests
from zipfile import ZipFile
import time
import os
import sys
from datetime import datetime
from logbook import Logger, StreamHandler, FileHandler
import threading
from windows_inhibitor import WindowsInhibitor


# prevent windows from sleeping while script is running
if os.name == 'nt':
    osSleep = WindowsInhibitor()
    osSleep.inhibit()

if 'logs' not in os.listdir():
    os.mkdir('logs')

stream_handler = StreamHandler(sys.stdout, format_string=" | {record.message}", bubble=True)
file_handler = FileHandler('logs/quandl_import_log_{}.log'.format(datetime.today().strftime("%Y-%m-%d_%H-%M")),
                           format_string=" | {record.message}", bubble=True)
log = Logger(__name__)
file_handler.push_application()
stream_handler.push_application()


api_key = os.environ.get('QUANDL_API_KEY')
quandl.ApiConfig.api_key = 'stBZDya6MDKyDMw4F4hg'

# Download and unzip file with list of all available contracts
# Files are being saved to working directory
r = requests.get('https://www.quandl.com/api/v3/databases/CME/metadata?api_key={}'.format(api_key))
r.raise_for_status()
log.info('Downloaded metadata file from quandl')

with open('CME_metadata.zip', 'wb') as file:
    file.write(r.content)
    log.info('Zipped file from quandl saved as CME_metadata.zip')

with ZipFile('CME_metadata.zip') as zip_file:
    file_names = zip_file.namelist()
    assert len(file_names) == 1, "Expected a single file from Quandl."
    file = file_names.pop()
    log.info('Zip contains file {}'.format(file))
    zip_file.extract(file)
    log.info('File extracted and saved in working directory')


# Download data for particular contracts
contracts = pd.read_csv('CME_metadata.csv')
# drop option codes which are mistakenly included in Quandl file
contracts.drop(contracts[contracts['code'].str.len() > 8].index, inplace=True)
# drop various indexes included in Quandl file
contracts.drop(contracts[contracts['code'].str.contains('INDEX', regex=False) == True].index, inplace=True)
contracts = contracts['code'].tolist()
log.info('Number of files to download: {}'.format(len(contracts)))

if 'data' not in os.listdir():
    os.mkdir('data')

def download_file(contract):
    fail_counter = 0
    # data directory must be empty before starting script
    while '{}.csv'.format(contract) not in os.listdir('data'):
        try:
            a = quandl.get('CME/{}'.format(contract))
            a.to_csv('data/{}.csv'.format(contract))
            global counter
            counter += 1
            log.info('wrote file no. {}: {}.csv'.format(counter, contract))
        except quandl.LimitExceededError:
            fail_counter += 1
            if fail_counter < 15:
                log.info('Quadle rate limit exceeded, thread sleeping for 5s')
                time.sleep(5)
            else:
                log.info('Seems you have exceeded quandl daily rate limit. Try again tommorrow.')
                sys.exit()

# used to count number of files downloaded    
counter = 0
start = time.time()
# used to get duration of every 10 downloads
sub_counter = 1
t = time.time()
times = []
for contract in contracts:
    while threading.active_count() > 3:
        time.sleep(1)
    thread = threading.Thread(target=download_file, args=(contract,))
    thread.start()
    sub_counter += 1
    if sub_counter % 10 == 0:
        log.info('Time to download 10 files: {}s'.format(time.time()-t))
        times.append(time.time()-t)
        log.info('Average: {}s'.format(np.mean(times)))
        t = time.time()
    
while counter < len(contracts):
    time.sleep(1)

end = time.time()
duration = end - start
h = int(duration/3600)
m = int(int(duration % 3600)/60)
s = int(duration % 3600) % 60
log.info('Finished! Wrote {} files in {}h{}m{}s'.format(counter, h, m, s))


