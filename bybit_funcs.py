from pybit.unified_trading import HTTP
import parameters as api
import pandas as pd
import calendar, datetime
from datetime import timezone, timedelta

# Creates a bybit session
def session():
    """
    Initializes a session with the Bybit API using the provided API key and secret.
    Returns:
        HTTP: An instance of the HTTP class for making API calls.
    """
    session = HTTP(
       api_key=api.bybit_key,
       api_secret=api.bybit_secret,
       testnet=False)       
    return session

# Returns list of ['unixTime*1000 (ms)', 'O', 'H', 'L', 'C', 'V', 'turnover'] over provided interval starting at provided datetime
def flatfile(symbol, start_time, **kwargs):
        """
    Fetches historical kline data for a specified trading symbol starting from a given datetime.
    Args:
        symbol (str): The trading symbol to fetch data for.
        start_time (datetime): The starting datetime for fetching kline data.
        **kwargs: Additional keyword arguments, including:
            - latest (bool): If True, fetches the latest kline data.
            - interval (str): The time interval for the kline data (default is '5' minutes).
    Returns:
        dict: A dictionary containing the symbol and its corresponding kline data.
    """
    latest = kwargs.get('latest', False)

    ######***** Feel free to change the interval to whatever you want! *****######
    interval = kwargs.get('interval', '5')

    activate = session()
    start = calendar.timegm(start_time.utctimetuple())*1000+(int(interval)*60000) # start = last recorded time + interval in ms
    now = calendar.timegm(datetime.datetime.now(tz=timezone(timedelta(hours=-8.0))).utctimetuple())*1000
    data = []
    # print(symbol)
    if not(latest):
        # While latest candle time from data list is < (now-5mins in ms) keep adding candles to the data list
        while int(start) <= (int(now) - (int(interval)*60000)): 
            try:
                # List of lists [['unixtime*1000','O','H','L','C','V','turnover'],[ ],...] *Note: The list is inversed, top is latest time
                temp = activate.get_kline(symbol=symbol, interval=interval, start=start).get('result').get('list')
            except:
                print(f'{symbol} doesnt work.')
                return
            temp.reverse()
            data.extend(temp)
            start = int(data[-1][0])+(int(interval)*60000) # Last unix time + interval in ms
            # now = calendar.timegm(datetime.datetime.now(tz=timezone(timedelta(hours=-8.0))).utctimetuple())*1000
        data.pop()
    # Return last 200 candles
    else:
        try:
            data.append(activate.get_kline(symbol=symbol, interval=interval).get('result').get('list'))
        except:
            print(f'{symbol} DF doesnt work.')
    return {symbol:data}

def frame(data):
    """
    Converts raw kline data into a pandas DataFrame for analysis.
    Args:
        data (list): A list of kline data, where each entry contains time, open, high, low, close, volume, and turnover.
    Returns:
        DataFrame: A pandas DataFrame with the kline data, indexed by time.
    """
    df = pd.DataFrame(data, columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'turnover'])
    f = lambda x: datetime.datetime.utcfromtimestamp(int(x)/1000)
    df.index = df.Time.apply(f)
    df = df.drop(columns=['turnover', 'Time'])
    # df[::-1].apply(pd.to_numeric) # This inverses the DF, putting the latest time at the bottom
    return df

# Gets a list of all currently tradeable linear perpetual USDT pairs
def get_assets():
    """
    Fetches a list of all currently tradeable linear perpetual USDT pairs from the Bybit API.
    Returns:
        list: A list of tradeable symbols, excluding untradeable pairs and stablecoins.
    """
    activate = session()
    symbols = []
    discard = ['USTCUSDT', 'USDCUSDT', 'BUSDUSDT', 'MAVIAUSDT', 'USDEUSDT'] # Eliminates any untradeable or Stablecoin pairs
    symbol_list = activate.get_instruments_info(category="linear").get('result').get('list')
    for symbol in range(len(symbol_list)):
        word = symbol_list[symbol].get("symbol")
        if word.find('-') != -1 or word.find('PERP') != -1 or word in discard: # Eliminates odd pairs that are not tradeable
            continue
        else: symbols.append(word)
    symbols.append('PERPUSDT') # Adds back in a PERPUSDT pair (may not be applicable in current market conditions ie delisted)
    return symbols
