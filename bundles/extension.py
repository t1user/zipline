import pandas as pd

from zipline.data.bundles import register as _register
from bundles.fut_bundle import futures_bundle

#start_session = pd.Timestamp('2000-1-3', tz='utc')
#end_session = pd.Timestamp.utcnow()

def register():
    return _register(
        'futures',
        futures_bundle,
    )

