# WORK IN PROGRESS

import pandas as pd
import numpy as np
import talib
from zipline.api import (order, record, symbol, continuous_future,
                         future_symbol, get_open_orders, order_target_percent,
                         set_slippage, set_commission)
from zipline.finance.slippage import FixedSlippage
from zipline.finance.commission import PerTrade
from contracts import contracts


FAST_MA = 50
SLOW_MA = 100
BREAKOUT = 50 # breakout beyond x days max/min
STOP = 2 # stop after x ATRs
RISK = .2 # % of capital daily risk per position

def initialize(context):
    set_slippage(us_futures=FixedSlippage(spread=0.25))
    set_commission(us_futures=PerTrade(0))
    context.contracts = [
        continuous_future(contract,
                          offset=0,
                          adjustment='mul',
                          roll='volume')
        for contract in contracts]

    context.min_max = {}

        

def handle_data(context, data):
    hist = data.history(context.contracts,
                        fields = ['price', 'high', 'low'],
                        bar_count = SLOW_MA + 10,
                        frequency = '1d')


    slow_ma = hist['price'].apply(lambda x: 
                                  talib.EMA(x.as_matrix(), 
                                            timeperiod=SLOW_MA)[-1])
    fast_ma = hist['price'].apply(lambda x: 
                                  talib.EMA(x.as_matrix(), 
                                            timeperiod=FAST_MA)[-1])
    atr = hist.apply(lambda x: talib.ATR(x['high'].fillna(x['price']).as_matrix(), 
                                         x['low'].fillna(x['price']).as_matrix(),
                                         x['price'].as_matrix(), 
                                         timeperiod=SLOW_MA)[-1], 
                     axis=(1,0))
    # breakout above is a buy signal
    upper = hist['price'][BREAKOUT:-1].max(axis=0)
    # breakout below is a sell signal
    lower = hist['price'][-BREAKOUT:-1].min(axis=0)
    # last price
    price = hist['price'].iloc[-1,:]
    
    #weights = price * context.risk_factor * context.weights 
    #weights = (RISK * context.weights * price) / (atr *100)

    weights = RISK * price / (STOP * atr * 100)
    
    longs = ((price > upper) & (fast_ma > slow_ma)) * weights 
    shorts = ((price < lower) & (fast_ma < slow_ma)) * -weights   
 
    
    positions = longs + shorts
      
    
    # Get the current contract of each of the futures.
    contracts = data.current(context.contracts, 'contract')
    
    positions.index = data.current(positions.index, 'contract')
    atr.index = data.current(atr.index, 'contract')
    price.index = data.current(price.index, 'contract')
    atr_dict = atr.to_dict()
    #price = data.current(atr.index, 'price') 

    # rollover expiring contracts
    for cont, pos in context.portfolio.positions.items():
        if cont not in positions.index:
            weight = context.portfolio.current_portfolio_weights[cont]
            root = cont.root_symbol
            # close existing contract
            positions[cont] = 0
            current = data.current(continuous_future(root), 'contract')
            # open current contract
            positions[current] = weight
            # still need the old contract for stop-loss calculation
            atr_dict[cont] = atr_dict[current]

        

    # implement stop-loss 
    stop_out = []        
    for contract, position in context.portfolio.positions.items():
        if contract in context.min_max:
            context.min_max[contract] = (min(context.min_max[contract][0],
                                             position.cost_basis,
                                             position.last_sale_price),
                                         max(context.min_max[contract][1],
                                             position.cost_basis,
                                             position.last_sale_price))
        else:
          context.min_max[contract] = (min(position.cost_basis,
                                           position.last_sale_price),
                                       max(position.cost_basis,
                                           position.last_sale_price))    

        # calculate stop loss level
        try:
            if position.amount > 0:
                stop_price = context.min_max[contract][1] - atr_dict[contract] * STOP
                if price[contract] <= stop_price:
                    positions[contract] = 0
                    stop_out.append(contract)
                
            if position.amount < 0:
                stop_price = context.min_max[contract][0] + atr_dict[contract] * STOP
                if price[contract] >= stop_price:
                    positions[contract] = 0
                    stop_out.append(contract)
        except (KeyError, IndexError):
            pass
            #print('key error in stop-loss function: ', contract)
            #print(context.min_max)
            #print(atr_dict)

    temp = []        
    for key in context.min_max.keys():
        if key not in context.portfolio.positions.keys():
            temp.append(key)
    for key in temp:
        del context.min_max[key]
            
    # list of positions not to be affected by stop-loss
    current_positions = list(context.portfolio.positions.keys())
    for pos in stop_out:
        if pos in current_positions:
            current_positions.remove(pos)
          
    # fillna is a temporary fix
    weights_dict = positions.fillna(0).to_dict()
    for asset, target in weights_dict.items():
        if target != 0:
            order_target_percent(asset, target)
            
"""            
    open_orders = get_open_orders()
    for future in context.contracts:     
        contract = contracts[future]
        try:
            if data.can_trade(contract) and contract not in open_orders:  
                pass
        except:
            if future not in context.missing_futures:
                context.missing_futures.append(future)
                print(context.missing_futures)
    # Order the futures we want to the target weights we decided above.
    try:
        order_optimal_portfolio(
            opt.TargetWeights(positions),
            constraints=[opt.Frozen(current_positions)],
        )
    except:
        pass
"""
