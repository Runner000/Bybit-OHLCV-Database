import Update_DB.github.db_funcs as db
import Update_DB.github.bybit_funcs as bybit
import time
from time import perf_counter
import parameters as api
import concurrent.futures, discord
import Update_DB.github.analysis_funcs as anal
import nest_asyncio
nest_asyncio.apply()

TOKEN = api.discord_token
client = discord.Client(intents = discord.Intents.default())

# Grabs all bybit perp assets, grabs last recorded candle in DB, grabs bybit kline data from last record, inserts new candels into DB
def update_db():
    start_time_db = perf_counter()
    counter = 0
    assets = bybit.get_assets()
    symbol_times = db.symbol_times(assets)
    symbols = []
    start_times = []
    all_data = [] #List of dicts {symbol:list of [tohlcv]}

    # Check for delisted symbols and delete them from the DB every 24 hours
    if time.localtime()[3] == [16,17]:
        db.delistCheck()

    for symbol in symbol_times:
        symbols.append(symbol)
        start_times.append(symbol_times[symbol])
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(bybit.flatfile, symbols, start_times)
        for result in results:
            counter +=1
            all_data.append(result)
            print(f"Grabbed data for {list(result.keys())[0]}       Task Percentage Done: {round(counter/len(symbols)*100,2)}%")
    df_list = []
    counter = 0
    for item in range(len(all_data)):
        counter +=1
        dic = all_data[item]
        symbol = list(dic.keys())[0]
        df = bybit.frame(dic.get(symbol))
        df_list.append({symbol:df})
        # print(f"Framed {symbol}       Task Percentage Done: {round(counter/len(all_data)*100,4)}%")
    db.bulk_db_insert(df_list)
    end_time_db = perf_counter()
    print(f"\nTotal run time of Signal Scan: {round((end_time_db-start_time_db)/60,2)} mins")

@client.event
async def on_ready():

    guild = discord.utils.get(client.guilds, name='Trading')
    channel = discord.utils.get(guild.text_channels, name='🌐│ema-scans')
    for payload in payloads:
        await channel.send(payload)
    await client.close()

def data_scans():
    start_time_scans = perf_counter()

    print("Starting EMA Scans:\n")
    p1, p2 = anal.alts_scan()
    scans = [p1, p2, anal.btc_scan(), anal.eth_scan()]

    end_time_scans = perf_counter()
    print(f"\nTotal run time of Signal Scan: {round((end_time_scans-start_time_scans)/60,2)} mins")
    return scans

start_time_total = perf_counter()
print("\nStarting Database Updates\n")
update_db()
payloads = data_scans()
client.run(TOKEN)
end_time_total = perf_counter()
print(f"\nTotal run time of Signal Scan: {round((end_time_total-start_time_total)/60,2)} mins")



