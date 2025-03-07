from pybit.unified_trading import HTTP
import parameters as api
import pandas as pd
import calendar, datetime
from datetime import timezone, timedelta

# Creates a bybit session
def session():
    session = HTTP(
       api_key=api.bybit_key,
       api_secret=api.bybit_secret,
       testnet=False)       
    return session

# Returns list of ['unixTime*1000 (ms)', 'O', 'H', 'L', 'C', 'V', 'turnover'] over provided interval starting at provided datetime
def flatfile(symbol, start_time, **kwargs):
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
    df = pd.DataFrame(data, columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'turnover'])
    f = lambda x: datetime.datetime.utcfromtimestamp(int(x)/1000)
    df.index = df.Time.apply(f)
    df = df.drop(columns=['turnover', 'Time'])
    # df[::-1].apply(pd.to_numeric) # This inverses the DF, putting the latest time at the bottom
    return df

# Gets a list of all currently tradeable linear perpetual USDT pairs
def get_assets():
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