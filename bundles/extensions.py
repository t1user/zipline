import pandas as pd

from zipline.data.bundles import register as _register
from bundles.fut_bundle import csvdir_futures

start_session = pd.Timestamp('2000-1-1', tz='utc')
end_session = pd.Timestamp('2018-10-1', tz='utc')


def register():
    return _register(
        'futures',
        csvdir_futures(
            ['daily'],
            '../data',
        ),
        calendar_name='NYSE',
        #start_session=start_session,
        #end_session=end_session
    )

