# Add API KEY to the Token Header or import from a .py file
# Get A Series of Historical Option Quotes from the API

# Example usage:
# get_options_quote_from('IBM', '2023-02-10', 'C', '140', '2023-01-01')

import requests
import pandas as pd
import numpy as np
import json
from datetime import datetime
import api_key

def get_options_chainbyExpr(symbol, expiry_date):
    # mf.get_options_chainbyExpr('SPXW', '2023-02-17')

    url = 'https://api.marketdata.app/v1/options/chain/'
    
    #expiry_date = datetime.strptime(expiry_date, '%Y-%m-%d')
    #expiry_date = expiry_date.strftime('%y%m%d')
    date_format = 'timestamp'
    headers = {'Authorization': api_key.API_KEY}
    

    path = f'{symbol}/?exiration={expiry_date}&dateformat=timestamp'

    final_url = url + path
    chain_expr = requests.get(final_url, headers=headers)

    return chain_expr.text

def get_options_chainbyMonth(symbol):
    # mf.get_options_chainbyMonth('SPX')
    #https://api.marketdata.app/v1/options/chain/AAPL/?monthly=true&dateformat=timestamp
    
    url = 'https://api.marketdata.app/v1/options/chain/'  #AAPL/?monthly=true&dateformat=timestamp
    
    
    headers = {'Authorization': api_key.API_KEY}

    path = f'{symbol}/?monthly=true&dateformat=timestamp'

    final_url = url + path
    chain_expr = requests.get(final_url, headers=headers)
    return chain_expr.text



def get_hist_quotes_to_from(symbol_list, from_date, to_date):
    # mf.get_hist_quotes_to_from(spx_list, '2023-01-26', '2023-02-04')
    
    #https://api.marketdata.app/v1/options/quotes/AAPL250117C00150000/?from=2023-01-01&to=2023-01-31&dateformat=timestamp
    
    url = 'https://api.marketdata.app/v1/options/quotes/'
    
    headers = {'Authorization': api_key.API_KEY}

    symbols = symbol_list

    # Initialize an empty DataFrame to hold the data
    df = pd.DataFrame()

    # Iterate through the symbols, making a request for each one
    for symbol in symbols:
        # Build the path by concatenating the symbol, date and date_format
        path = f'{symbol}/?from={from_date}&to={to_date}&dateformat=timestamp'
        # Concatenate the base URL and the path
        final_url = url + path
        # Make the request
        response = requests.get(final_url, headers=headers)
        # Extract the data from the response
        data = response.json()
        # Create a new DataFrame with the data and a column for the symbol
        symbol_df = pd.DataFrame({'updated': data.get('updated', np.nan),
                         'symbol': symbol,
                         'bid': data.get('bid', np.nan),
                         'bidSize': data.get('bidSize', np.nan),
                         'mid': data.get('mid', np.nan),
                         'ask': data.get('ask',np.nan),
                         'askSize': data.get('askSize', np.nan),
                         'last': data.get('last', np.nan),
                         'openInterest': data.get('openInterest', np.nan),
                         'volume': data.get('volume', np.nan),
                         'inTheMoney': data.get('inTheMoney', np.nan),
                         'intrinsicValue': data.get('intrinsicValue', np.nan),
                         'extrinsicValue': data.get('extrinsicValue', np.nan),
                         'underlyingPrice': data.get('underlyingPrice', np.nan)})


        # Add a new column to symbol_df with the id and date values concatenated
        symbol_df['id'] = symbol + '_' + symbol_df['updated'].astype(str)
        # Use the new column as the index when concatenating with df
        df = pd.concat([df, symbol_df.set_index('id')], ignore_index=False, sort=False, axis=0)
    return df

    # GET CANDLES  ###NEED TO FINISH DATE AND TIME COLUMNS

def get_candles(symbol, from_date, to_date, res):
    # mf.get_candles('MSFT', '2023-02-03', '2023-02-04', 'D')
    
    # https://api.marketdata.app/v1/stocks/candles/D/AAPL?from=2020-01-01&to=2020-12-31

    """ :Date Format: ('2023-01-15')
        :Minutely Resolutions: (minutely, 1, 3, 5, 15, 30, 45, ...)
        :Hourly Resolutions: (hourly, H, 1H, 2H, ...)
        :Daily Resolutions: (daily, D, 1D, 2D, ...)
        :Weekly Resolutions: (weekly, W, 1W, 2W, ...)
        :Monthly Resolutions: (monthly, M, 1M, 2M, ...)
        :Yearly Resolutions:(yearly, Y, 1Y, 2Y, ...)
    """
        
    url = 'https://api.marketdata.app/v1/stocks/candles/'  #AAPL/?monthly=true&dateformat=timestamp
    
    
    headers = {'Authorization': api_key.API_KEY}

    path = f'{res}/{symbol}/?from={from_date}&to={to_date}&dateformat=timestamp'

    final_url = url + path
    candles = requests.get(final_url, headers=headers)
    candles = candles.text
    candles_pd = json.loads(candles)
    candles_hist = pd.DataFrame(candles_pd)
    candles_hist['symbol'] = symbol
       
    # rename the columns
    columns = {
        'c': 'close',
        'h': 'high',
        'l': 'low',
        'o': 'open',
        'v': 'volume',
        't': 'date'
    }
    candles_hist.rename(columns=columns, inplace=True)
    candles_hist.drop(['s'], axis=1, inplace=True)
    candles_hist = candles_hist.reindex(columns=['symbol','date', 'close', 'high', 'low', 'open', 'volume'])
    return candles_hist


def get_candles_from(symbol, from_date, res):
    # mf.get_candles('MSFT', '2020-02-03','D')
    
    # url2 = "https://api.marketdata.app/v1/stocks/candles/D/AAPL?from=2020-12-31&countback=252"

    """ :Date Format: ('2023-01-15')
        :Minutely Resolutions: (minutely, 1, 3, 5, 15, 30, 45, ...)
        :Hourly Resolutions: (hourly, H, 1H, 2H, ...)
        :Daily Resolutions: (daily, D, 1D, 2D, ...)
        :Weekly Resolutions: (weekly, W, 1W, 2W, ...)
        :Monthly Resolutions: (monthly, M, 1M, 2M, ...)
        :Yearly Resolutions:(yearly, Y, 1Y, 2Y, ...)
    """
        
    url = 'https://api.marketdata.app/v1/stocks/candles/'  #AAPL/?monthly=true&dateformat=timestamp
    
    
    headers = {'Authorization': api_key.API_KEY}

    #path = f'{res}/{symbol}/?from={from_date}&countback={count_back}&dateformat=timestamp'
    path = f'{res}/{symbol}/?from={from_date}&dateformat=timestamp'

    final_url = url + path
    candles = requests.get(final_url, headers=headers)
    candles = candles.text
    candles_pd = json.loads(candles)
    candles_hist = pd.DataFrame(candles_pd)
    candles_hist['symbol'] = symbol
       
    # rename the columns
    columns = {
        'c': 'close',
        'h': 'high',
        'l': 'low',
        'o': 'open',
        'v': 'volume',
        't': 'date'
    }
    candles_hist.rename(columns=columns, inplace=True)
    candles_hist.drop(['s'], axis=1, inplace=True)
    candles_hist = candles_hist.reindex(columns=['symbol','date', 'close', 'high', 'low', 'open', 'volume'])
    return candles_hist