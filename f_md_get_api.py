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

def build_option_symbol(symbol, expiry_date, option_type, strike):
    
	"""
	Build an Option Symbol

	example       : build_option_symbol('IBM', '2023-02-10', 'C', 140)
	symbol        : Symbol of the underlying asset
	expiry_date   : Expiry date of the option in the format 'YYYY-MM-DD'
	option_type   : Option type, either 'C' (Call) or 'P' (Put)
	strike_price  : Strike price of the option, enter as number ('no quotes', ie 100)
			  
	"""
	# Remove hyphens from the expiry date
	expiry_date = expiry_date.replace('-', '')

	# Format the year as '23'
	year_formatted = expiry_date[2:]

	# Pad the strike price to a 7-digit string with leading zeroes
	# By converting to cents and padding it with a 0
	strike_formatted = f"{int(strike * 100):07d}0"

	option_symbol = f'{symbol}{year_formatted}{option_type[0].upper()}{strike_formatted}'
	return option_symbol

def get_option_chain_live(symbol):
    """
    get_options_chain_live('AAPL')
    symbol         :'AAPL'
    """
    # https://api.marketdata.app/v1/options/chain/AAPL/?dateformat=timestamp

    url = 'https://api.marketdata.app/v1/options/chain/' 
 
    headers = {'Authorization': api_key.API_KEY}
    path = f'{symbol}/?dateformat=timestamp'
    final_url = url + path
    chain_expr = requests.get(final_url, headers=headers)
    
    return chain_expr.text

def get_options_chain_from(symbol, from_date):
    """
    get_options_chain_expr('AAPL', '2023-02-17')
    symbol         :'AAPL'
    from_date    :'2023-02-17'
    """

    #https://api.marketdata.app/v1/options/chain/AAPL/?date=2020-01-06&dateformat=timestamp

    url = 'https://api.marketdata.app/v1/options/chain/'
        
    date_format = 'timestamp'
    headers = {'Authorization': api_key.API_KEY}
    
    path = f'{symbol}/?date={from_date}&dateformat=timestamp'

    final_url = url + path
    chain_expr = requests.get(final_url, headers=headers)

    return chain_expr.text


def get_options_chain_expr(symbol, expiry_date):
    """
	Current Option Chains, See
	get_options_chain_expr_from(symbol, expiry_date, from_date
	for Expr Chains
    get_options_chain_expr('AAPL', '2023-02-17')
    symbol         :'AAPL'
    expiry_date    :'2023-02-17'
    """

    # https://api.marketdata.app/v1/options/chain/AAPL/?expiration=2024-01-19&dateformat=timestamp

    url = 'https://api.marketdata.app/v1/options/chain/'
        
    date_format = 'timestamp'
    headers = {'Authorization': api_key.API_KEY}
    
    path = f'{symbol}/?expiration={expiry_date}&dateformat=timestamp'

    final_url = url + path
    chain_expr = requests.get(final_url, headers=headers)

    return chain_expr.text
	
def get_options_chain_expr_from(symbol, expiry_date, from_date):
    """
	Expired Option Chains, See
	get_options_chain_expr(symbol, expiry_date)
	for Current Chains
    get_options_chain_expr('AAPL', '2023-02-17')
    symbol         :'AAPL'
    expiry_date    :'2023-02-17'
    """

    # https://api.marketdata.app/v1/options/chain/AAPL/?expiration=2024-01-19&dateformat=timestamp  #Current Chains
	# https://api.marketdata.app/v1/options/chain/AAPL/?expiration=2023-03-10&date=2023-03-10&dateformat=timestamp  #Expr Chains

    url = 'https://api.marketdata.app/v1/options/chain/'
        
    date_format = 'timestamp'
    headers = {'Authorization': api_key.API_KEY}
    
    path = f'{symbol}/?expiration={expiry_date}&date={from_date}&dateformat=timestamp'

    final_url = url + path
    chain_expr = requests.get(final_url, headers=headers)

    return chain_expr.text	
	
	

def get_options_chain_weekly(symbol):
    """
    get_options_chain_weekly('SPX')
    symbol:     :'SPX'

    """
    # https://api.marketdata.app/v1/options/chain/AAPL/?monthly=false&dateformat=timestamp
    
    url = 'https://api.marketdata.app/v1/options/chain/'  #AAPL/?monthly=true&dateformat=timestamp
    
    
    headers = {'Authorization': api_key.API_KEY}

    path = f'{symbol}/?monthly=false&dateformat=timestamp'

    final_url = url + path
    chain_expr = requests.get(final_url, headers=headers)
    return chain_expr.text

def get_options_chain_monthly(symbol):
    """
    get_options_chain_monthly('SPX')
    symbol:     :'SPX'
    """
    # https://api.marketdata.app/v1/options/chain/AAPL/?monthly=true&dateformat=timestamp
    
    url = 'https://api.marketdata.app/v1/options/chain/'  #AAPL/?monthly=true&dateformat=timestam
    
    
    headers = {'Authorization': api_key.API_KEY}

    path = f'{symbol}/?monthly=true&dateformat=timestamp'

    final_url = url + path
    chain_expr = requests.get(final_url, headers=headers)
    return chain_expr.text

def get_options_chain_year(symbol, year):
    """
    get_options_chain_year('IBM', '2022')
    symbol      :'IBM'
    year        :'2023'
    """
    # https://api.marketdata.app/v1/options/chain/AAPL/?year=2022&dateformat=timestamp
    
    url = 'https://api.marketdata.app/v1/options/chain/'  #AAPL/?monthly=true&dateformat=timestamp    
    
    headers = {'Authorization': api_key.API_KEY}

    path = f'{symbol}/?year={year}&dateformat=timestamp'

    final_url = url + path
    chain_expr = requests.get(final_url, headers=headers)
    return chain_expr.text

def get_options_chain_strike(symbol, strike):
    """
    get_options_chain_strike('IBM', '150')
    symbol      :'IBM'
    strike      :'150'
    """
    # https://api.marketdata.app/v1/options/chain/AAPL/?strike=150&dateformat=timestamp
    
    url = 'https://api.marketdata.app/v1/options/chain/'  #AAPL/?monthly=true&dateformat=timest
        
    headers = {'Authorization': api_key.API_KEY}

    path = f'{symbol}/?strike={strike}&dateformat=timestamp'

    final_url = url + path
    chain_expr = requests.get(final_url, headers=headers)
    return chain_expr.text

def get_hist_quotes_from_to(symbol_list, from_date, to_date):
    """
    get_options_chain_strike(spx_list, '2023-01-26', '2023-02-04')
    symbol_list      :['SPX230217C04100000','SPX230217P04100000']
    from_date        :'2023-01-26'
    to_date          :'2023-02-04'
    """
    # This function still needs a bit of work.  Getting an error message when going to far out on the term.  thinking it may be that
    # the OptionSymbol is listed, but no quote...  Need to study future...  But Seems to work ok except for those exceptions.

    # https://api.marketdata.app/v1/options/quotes/AAPL250117C00150000/?from=2023-01-01&to=2023-01-31&dateformat=timestamp
    
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

def get_hist_quotes_from(symbol_list, from_date):
    """
    get_options_chain_strike(spx_list, '2023-01-26')
    symbol_list      :['SPX230217C04100000','SPX230217P04100000']
    from_date        :'2023-01-26'
    """
    # This function still needs a bit of work.  Getting an error message when going to far out on the term.  th
    # the OptionSymbol is listed, but no quote...  Need to study future...  But Seems to work ok except for tho

    # https://api.marketdata.app/v1/options/quotes/AAPL250117C00150000/?from=2023-01-01&to=2023-01-31&dateformat=timestamp
    
    url = 'https://api.marketdata.app/v1/options/quotes/'
    
    headers = {'Authorization': api_key.API_KEY}

    symbols = symbol_list

    # Initialize an empty DataFrame to hold the data
    df = pd.DataFrame()

    # Iterate through the symbols, making a request for each one
    for symbol in symbols:
        # Build the path by concatenating the symbol, date and date_format
        path = f'{symbol}/?from={from_date}&dateformat=timestamp'
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


def get_options_quote_live(symbol, expiry_date, option_type, strike_price):
    """
    Get Live quotes of an option from a beginning date.
    example       : options_quote_live('IBM', '2023-02-10', 'C', '140')
    symbol        : Symbol of the underlying asset
    expiry_date   : Expiry date of the option in the format 'YYYY-MM-DD'
    option_type   : Option type, either 'C' (Call) or 'P' (Put)
    strike_price  : Strike price of the option    
        
    Returns:
        JSON response from the API.
    """
    expiry_date = datetime.strptime(expiry_date, '%Y-%m-%d')
    expiry_date = expiry_date.strftime('%y%m%d')
    strike_price = '{:0>8}'.format(int(strike_price) * 1000)
    option_symbol = f'{symbol}{expiry_date}{option_type}{strike_price}'

    url = 'https://api.marketdata.app/v1/options/quotes/'
    
    headers = {'Authorization': api_key.API_KEY}

    path = f'{option_symbol}/?dateformat=timestamp'
    final_url = url + path

    chain_response = requests.get(final_url, headers=headers)
    return chain_response.text

    #print(final_url)

def get_candles_from_to(switch, symbol, from_date, to_date, res):
    """
    :get_candles('s', 'MSFT', '2023-02-03', '2023-02-04', 'D')
    :switch     : 's', stocks | 'i', indices
    :Date Format: ('2023-01-15')
    :Minutely Resolutions: (minutely, 1, 3, 5, 15, 30, 45, ...)
    :Hourly Resolutions: (hourly, H, 1H, 2H, ...)
    :Daily Resolutions: (daily, D, 1D, 2D, ...)
    :Weekly Resolutions: (weekly, W, 1W, 2W, ...)
    :Monthly Resolutions: (monthly, M, 1M, 2M, ...)
    :Yearly Resolutions:(yearly, Y, 1Y, 2Y, ...)
    """

    def url_switch(switch):
        if switch == 's':
            url = 'https://api.marketdata.app/v1/stocks/candles/'
        elif switch == 'i':
            url = 'https://api.marketdata.app/v1/indices/candles/'
        else:
            return "Invalid switch parameter"
        return url

    # https://api.marketdata.app/v1/stocks/candles/D/AAPL?from=2020-01-01&to=2020-12-31

    url = url_switch(switch)

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


def get_candles_from(switch, symbol, from_date, res):
    """
    :get_candles('s', 'MSFT', '2023-02-03', 'D')
    :switch     : 's', stocks | 'i', indices
    :Date Format: ('2023-01-15')
    :Minutely Resolutions: (minutely, 1, 3, 5, 15, 30, 45, ...)
    :Hourly Resolutions: (hourly, H, 1H, 2H, ...)
    :Daily Resolutions: (daily, D, 1D, 2D, ...)
    :Weekly Resolutions: (weekly, W, 1W, 2W, ...)
    :Monthly Resolutions: (monthly, M, 1M, 2M, ...)
    :Yearly Resolutions:(yearly, Y, 1Y, 2Y, ...)
    """

    def url_switch(switch):
        if switch == 's':
            url = 'https://api.marketdata.app/v1/stocks/candles/'
        elif switch == 'i':
            url = 'https://api.marketdata.app/v1/indices/candles/'
        else:
            return "Invalid switch parameter"
        return url

    # url = 'https://api.marketdata.app/v1/stocks/candles/'

    url = url_switch(switch)

    # https://api.marketdata.app/v1/stocks/candles/D/AAPL/?from=2023-02-01&dateformat=timestamp  
    
    headers = {'Authorization': api_key.API_KEY}

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


def get_stock_quote_live(switch, symbol):
    """
    get_stock_quote_live('s', 'AAPL')

    switch         :'s', stock | 'i', indices
    symbol         :'AAPL'
    """
    def url_switch(switch):
        if switch == 's':
            url = 'https://api.marketdata.app/v1/stocks/quotes/'
        elif switch == 'i':
            url = 'https://api.marketdata.app/v1/indices/quotes/'
        else:
            return "Invalid switch parameter"
        return url



    # https://api.marketdata.app/v1/stocks/quotes/AAPL/?dateformat=timestamp

    url = url_switch(switch)
 
    headers = {'Authorization': api_key.API_KEY}
    path = f'{symbol}/?dateformat=timestamp'
    final_url = url + path
    chain_expr = requests.get(final_url, headers=headers)
    
    return chain_expr.text