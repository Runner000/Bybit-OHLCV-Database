import statistics, pandas_ta as ta, tulipy as ti, numpy as np, connectorx as cx
from datetime import datetime, timedelta
from sqlalchemy import create_engine, inspect
from scipy.signal import savgol_filter, find_peaks
from parameters import db_path

# Resample OHLC data to whatever interval is passed
def resamp(df, interval):
    df = df.resample(interval, on='Time').agg({'Open':'first', 'High':'max', 'Low':'min', 'Close':'last', 'Volume':'sum'})
    return df

# Grabs the last amount of candles for symbol from DB
def grab_df(symbol, hours, candles, time1, time2):
        qry = f"""SELECT * FROM '{symbol}' WHERE (Time >= '{time2}' and Time <= '{time1}')"""
        df = cx.read_sql(db_path, query=qry)
        try:
            df = resamp(df, str(hours)+'H')
        except:
            print(f"{symbol} resample failed")
        if len(df)<candles:
            return False
        else: return df

def dfTimes(hour,candles):
    num_candles = int((12*hour)*candles)
    time1 = cx.read_sql(db_path, f'SELECT * FROM aaa1INCHUSDT ORDER BY Time DESC LIMIT 1').values[0][0].to_pydatetime() + timedelta(minutes=1)
    time2 = cx.read_sql(db_path, f"SELECT * FROM BTCUSDT ORDER BY Time DESC LIMIT {num_candles}").values[-1][0].to_pydatetime()
    return time1, time2
#----------------------------------------------------------------------------- INDICATORS -----------------------------------------------------------------------------
# Returns numpy array with EMA values
def EMA(Close, period):
    array = ta.ema(Close, period, fillna=0)
    return array

def RSI(close, window):
    series = ti.rsi(close.to_numpy(),window)
    pad = np.pad(series, ((len(close)-len(series)), 0), 'constant')
    return pad

def AO(high, low, fast, slow):
    hl =(high + low)/2
    ao = ta.ao(high, low)
    ao = ta.sma(hl, fast) - ta.sma(hl, slow)
    diff = [0]
    for item in range(len(ao)):
        if item == 0:
            continue
        elif ao.iloc[item] - ao.iloc[item-1] >= 0:
            diff.append(True)
        else: diff.append(False)
    return np.array(diff)

def obv_ema(close, volume, length):
    obv = ti.obv(close.to_numpy(), volume.to_numpy())
    # print(obv[-5:-1])
    series = ti.ema(obv, 18)
    pad = np.pad(series, ((len(close)-len(series)), 0), 'constant')
    # sign = [True if x>=y else False for x in obv for y in series]
    sign=[]
    for item in range(len(obv)):
        if obv[item]>= series[item]:
            sign.append(True)
        else: sign.append(False)
    # print(sign[-5:-1])
    return np.array(sign)

def above_ema(Close, period):
    ema = EMA(Close, period)
    last_ema = ema.iloc[-1]
    last_close = Close.iloc[-1] 
    if last_close > last_ema:
        return True
    else: return False

# Checks to see if the fast above the slow EMA
def emaCheck(close, slowL, fastL):
    slowEMA = EMA(close, slowL)
    fastEMA = EMA(close, fastL)
    if fastEMA.iloc[-1] > slowEMA.iloc[-1]:
        return True
    else: return False

def VWMA(close, volume, length):
    series = ta.vwma(close, volume, length, fillna=0)
    return series

def ATR(High, Low, Close, period):
    high, low, close = High.to_numpy(), Low.to_numpy(), Close.to_numpy()
    series = ti.atr(high, low, close, period)
    pad = np.pad(series, ((len(close)-len(series)), 0), 'constant')
    return pad

def slope(series, lookback, **kwargs):
    pcnt = kwargs.get('pcnt', 20)/100
    medianRange = kwargs.get('medianRange', 200)
    slopes = []
    
    for i in range(lookback, len(series),1):
        slopes.append((series.iloc[i]-series.iloc[i-lookback])*10)
    # median = ta.median(slopes[-200:], 199)[-1]
    median = statistics.median(slopes[-medianRange:])
    if slopes[-1] < pcnt*median and slopes[-1] > -pcnt*median:
        trend = 2
    elif slopes[-1] > 0:
        trend = 1
    elif slopes[-1] < 0:
        trend = 0
    return trend

def SARSI(close, length, multiplier, smooth_p):
    rsi = round(ta.rsi(close, length, fillna=0),3)
    sma = ta.sma(rsi, length, fillna=0)
    difference = abs(rsi - sma)
    method = round(ta.sma(difference,length, fillna=0),3)
    ob = 50 + multiplier*method
    os = 50 - multiplier*method
    smooth = round(ta.ema(rsi, smooth_p, fillna=0),3)
    i = 0

    if rsi.iloc[i-2] < os.iloc[i-2] and rsi.iloc[i-1] >= os.iloc[i-1] and rsi.iloc[i-1] >= smooth.iloc[i-1]:
        return "Bull"
    elif rsi.iloc[i-2] > ob.iloc[i-2] and rsi.iloc[i-1] <= os.iloc[i-1] and rsi.iloc[i-1] <= smooth.iloc[i-1]:
        return "Bear"
    else: 
        return False

def ema_cross(Close, period1, period2):
    fast_ema = EMA(Close,period1)
    slow_ema = EMA(Close,period2)
    if round(fast_ema.iloc[-2],5) <= round(slow_ema.iloc[-2],5) and round(fast_ema.iloc[-1],5) > round(slow_ema.iloc[-1],5):
        return 'bull'
    elif round(fast_ema.iloc[-2],5) >= round(slow_ema.iloc[-2],5) and round(fast_ema.iloc[-1],5) < round(slow_ema.iloc[-1],5):
        return 'bear'
    else: return False

def BB(df, period, stddev, **kwargs):
    bw = kwargs.get('bw', False)
    bands = ta.bbands(df.Close, period, stddev, fillna=0)
    if bw:
        bands = bands.drop(columns=[f'BBL_{period}_{stddev}', f'BBM_{period}_{stddev}', f'BBU_{period}_{stddev}'])
        return bands
    else:
        bands = bands.drop(columns=[f'BBB_{period}_{stddev}', f'rsi_{period}_{stddev}'])
        return bands
#----------------------------------------------------------------------------- STRATEGIES -----------------------------------------------------------------------------
def Combo(df):
    rsi = RSI(df.Close, 14)
    diff = AO(df.High, df.Low, 5, 32)
    obvColor = obv_ema(df.Close, df.Volume, 18)
    combo = []

    for x in range(len(rsi)):
        if obvColor[x] and rsi[x] >= 48 and diff[x]:
            combo.append("Bullish")
        elif not(obvColor[x]) and rsi[x] <= 48 and not(diff[x]):
            combo.append("Bearish")
        else: combo.append("Chop")

    return combo

def MS(df):
    atr = ATR(df.High, df.Low, df.Close, 14)
    # Smooths the Close into a line chart, finds the peaks and troughs of the line
    smooth = savgol_filter(df.Close, 20, 8)
    peaks_idx = find_peaks(smooth, distance=5, width=3, prominence=atr)[0]
    troughs_idx = find_peaks(-1*smooth, distance=5, width=3, prominence=atr)[0]

    up_run_len = 0
    up_run=True
    down_run_len = 0
    down_run=True
    #With the indexes of each peak and trough, we determine if each one is greater than or less than. This determines the nature of the trend (up/down)
    while up_run:
        if 2 + up_run_len > len(peaks_idx) or 2 + up_run_len > len(troughs_idx):
            break
        if smooth[peaks_idx[-1-up_run_len]] > smooth[peaks_idx[-2-up_run_len]] and smooth[troughs_idx[-1-up_run_len]] > smooth[troughs_idx[-2-up_run_len]]:
            up_run_len+=1
            int(up_run_len)
        else: up_run = False
    while down_run:
        if 2 + down_run_len > len(peaks_idx) or 2 + down_run_len > len(troughs_idx):
            break
        if smooth[peaks_idx[-1-down_run_len]] < smooth[peaks_idx[-2-down_run_len]] and smooth[troughs_idx[-1-down_run_len]] < smooth[troughs_idx[-2-down_run_len]]:
            down_run_len+=1
            int(down_run_len)
        else: down_run = False

    if up_run_len > 0:
        trend = 'Uptrend'
    elif down_run_len > 0:
        trend = 'Downtrend'
    else: trend = 'Chop'
    return trend

def insideDay(df):
    if df.High.iloc[(-1)] < df.High.iloc[(-2)] and df.Low.iloc[(-1)] > df.Low.iloc[(-2)]:
        return True
    else: 
        return False

def breakout(df):
    def findHigh(df):
        high = 0
        for row in range(len(df)):
            if df.Close.iloc[row] > high:
                high = df.Close.iloc[row]
        return high

    last = df[-51:-1]
    lastHigh = findHigh(last)
    if df.Close.iloc[-1] > lastHigh:
        return True
    else: return False

def alts_scan(**kwargs):
    candles = kwargs.get('candles', 300)
    symbols = kwargs.get('symbols', inspect(create_engine(db_path)).get_table_names())

    time1_24, time2_24 = dfTimes(24, 55)
    time1_4, time2_4 = dfTimes(4, candles)

    counter = 0
    # bull = []
    # bear = []
    inside = []
    trend_bull = []
    trend_bear = []
    broken = []

    for symbol in symbols:
        try: 
            if symbol in ['BTCUSDT', 'ETHUSDT']:    # Skip BTC and ETH, they have their own scanners
                continue
            # Finding Daily df
            if type(df_24 := grab_df(symbol, 24, 25, time1_24, time2_24)) == bool:
                print(f'\n{symbol} scan failed.')
                continue
            elif float(df_24.Volume.iloc[-2])*float(df_24.Open.iloc[-1]) < 5000000:
                print(f"\n{symbol} does not meet Volume criteria (5m).")
                continue
            # df = grab_df(symbol, hour, candles, time1, time2)
            df_4 = grab_df(symbol, 4, candles, time1_4, time2_4)
            if symbol[:3] == 'aaa':                 # Correct symbol name to clean up print statements
                symbol = symbol[3:]
            
            if insideDay(df_24) and datetime.now().hour in [16, 17]:
                inside.append(symbol)
            
            if breakout(df_24) and datetime.now().hour in [16, 17]:
                broken.append(symbol)

            if datetime.now().hour in [0, 4, 8, 12, 16, 20]:
                combo = Combo(df_4)
                if combo[-1] == "Bullish" and combo[-2] != "Bullish":
                    trend_bull.append(symbol)
                elif combo[-1] == "Bearish" and combo[-2] != "Bearish":
                    trend_bear.append(symbol)

        except Exception as e:
            print(e)
            print(f'\n{symbol} scan failed.')
            continue
            
        title = "{:<15}".format(symbol)
        print(f"Analyzing {title}                 Task Percentage Done: {round(counter / len(symbols),4)}%".format(5), end='\r', flush=True)
        counter += 1

    payload1 = ("\n"
               "__Alt Scans__\n"
               f"4H Trend Signals (Combo):\n"
               f"   Bull: {trend_bull}\n"
               f"   Bear: {trend_bear}\n")

    if datetime.now().hour in [16, 17]:
        payload1 =   (f"\n   Inside Day: {inside}"
                      f"\n   Breakouts: {broken}")
    else: payload2 = (f"\n   Inside Day/ Breakouts: See start of Day")
    return payload1, payload2

def btc_scan():
    print('Scanning BTC')
    candles = 300
    time1_4, time2_4 = dfTimes(4, candles)
    df_4 = grab_df('BTCUSDT', 4, candles, time1_4, time2_4)
    time1_24, time2_24 = dfTimes(24, 5)
    df_24 = grab_df('BTCUSDT', 24, 5, time1_24, time2_24)
    
    if ema_cross(df_4.Close, 12, 21):
        emacross = 'Recent Cross'
    else: emacross = None
    if above_ema(df_4.Close, 200):
        ema_200 = 'above'
    else: ema_200 = 'under'
    combo = Combo(df_4)[-1]

    payload = ("\n"
            "__BTC Scan__\n"
            f"  4H 12/21 EMA Trend: {emacross} and {ema_200} 200 EMA\n"
            f"  Inside Day: {insideDay(df_24)}\n"
            f"  4H Market structure: {MS(df_4.iloc[-120:])}")
    return payload

def eth_scan():
    print('Scanning ETH')
    candles = 300
    time1_4, time2_4 = dfTimes(4, candles)
    df_4 = grab_df('ETHUSDT', 4, candles, time1_4, time2_4)
    time1_24, time2_24 = dfTimes(24, 5)
    df_24 = grab_df('ETHUSDT', 24, 5, time1_24, time2_24)

    if ema_cross(df_4.Close, 12, 21):
        emacross = 'Recent Cross'
    else: emacross = None
    if above_ema(df_4.Close, 200):
        ema_200 = 'above'
    else: ema_200 = 'under'
    combo = Combo(df_4)[-1]

    payload = ("\n"
            "__ETH Scan__\n"
            f"  4H 12/21 EMA Trend: {emacross} and {ema_200} 200 EMA\n"
            f"  Inside Day: {insideDay(df_24)}\n"
            f"  4H Market structure: {MS(df_4.iloc[-120:])}")
    return payload
