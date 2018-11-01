import random
from zipline.api import order, record, symbol, continuous_future, future_symbol


def initialize(context):
    context.cf = continuous_future('ES', roll='volume', adjustment='add')
    context.counter = 0
    
def handle_data(context, data):
    """
    If there are no open positions, randomly buy or sell one contract. Hold for 10 days. Sell. 
    """
    if context.portfolio.positions:
        context.counter += 1
        positions = context.portfolio.positions
        asset_object = list(positions.keys())[0]
        position_object = positions[asset_object]
        record(cost = position_object.cost_basis,
               amount = position_object.amount)
        if context.counter < 10:
            return
        else:
            order(asset_object, -position_object.amount)
            context.counter = 0
    else:
        asset = data.current(context.cf, 'contract')
        amount = random.sample([-1, 1], 1)[0]
        order(asset, amount)
