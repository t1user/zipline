import pandas as pd

from zipline.data.bundles import register
from .fut_bundle import futures_bundle
from .settings import start_session, end_session


register(
    'futures',
    futures_bundle,
    calendar_name='NYSE',
    start_session=start_session,
    end_session=end_session,
    )

