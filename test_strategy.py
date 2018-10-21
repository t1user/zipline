from zipline.api import order_target, record, symbol, continuous_future, future_symbol

def initialize(context):
    #context.asset = future_symbol('CL')
    context.cf = continuous_future('CL')
    
def handle_data(context, data):
    #contract = data.history(context.cf, 'contract', bar_count=100, frequency="1d")

    # Save values for later inspection
    record(cf=data.current(context.cf, 'contract'),
           price=data.current(context.cf, 'close'))
