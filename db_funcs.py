import connectorx as cx, Update_DB.github.bybit_funcs as bybit
import datetime as dt
from sqlalchemy import create_engine, MetaData, Table, Column, inspect
import sqlalchemy as sql
from parameters import db_path

main_engine = create_engine(db_path)
   
def bulk_db_insert(df_list):
    for d in range(len(df_list)):
        dic = df_list[d]
        symbol = list(dic.keys())[0]
        df = dic[symbol]
        if not(symbol[0].isalpha()):
            title = 'aaa'+ symbol
        else: 
            title = f'{symbol}'
        df.to_sql(title, create_engine(db_path), if_exists='append', method='multi')
        print(str(len(df)) + f' new rows imported to {symbol} Table')
    return

# Returns a dict of dicts that contains the last times associated with each symbol table in the DB {symbol:maxtime}
def symbol_times(symbols):
    max_times = {}

    # Check if DB symbols are within the same 5m candle, by checking the first and last table. If so then write all the times to last table time. (quicker)
    top_time = cx.read_sql(db_path, f'SELECT * FROM aaa1INCHUSDT ORDER BY Time DESC LIMIT 1').values[0][0]
    bott_time = cx.read_sql(db_path, f'SELECT * FROM PERPUSDT ORDER BY Time DESC LIMIT 1').values[0][0]
    if top_time == bott_time:
        print("Grabbing symbol times quickly")
        for symbol in symbols:
        # Check if first characters are number because the DB doesnt support this
            if not(symbol[0].isalpha()):
                title = 'aaa'+ symbol
            else: 
                title = f'{symbol}'
            if not inspect(create_engine(db_path)).has_table(title):
                print(f'New table being created for {symbol}')
                meta = MetaData()
                # Create a table with the appropriate Columns
                Table(title, meta,
                    Column('Time', sql.DATETIME()), Column('Volume', sql.FLOAT()), Column('Open', sql.FLOAT()),
                    Column('High', sql.FLOAT()), Column('Low', sql.FLOAT()), Column('Close', sql.FLOAT()))
                # Implement the creation
                meta.create_all(create_engine(db_path))
                max_times[symbol] = dt.datetime(2020,1,1)
            # Table already exists, get the last rows datetime info
            else:
                max_times[symbol] = bott_time
        print("All symbol times grabbed")
        return max_times
    # Grabs each symbol time individually by looking at the last row in each table
    for symbol in symbols:
        # Check if first characters are number because the DB doesnt support this
        if not(symbol[0].isalpha()):
            title = 'aaa'+ symbol
        else: 
            title = f'{symbol}'
        # If table doesn't exist, create one, default maxtime=2020
        if not inspect(create_engine(db_path)).has_table(title):
            print(f'New table being created for {symbol}')
            meta = MetaData()
            # Create a table with the appropriate Columns
            Table(title, meta,
                Column('Time', sql.DATETIME()), Column('Volume', sql.FLOAT()), Column('Open', sql.FLOAT()),
                Column('High', sql.FLOAT()), Column('Low', sql.FLOAT()), Column('Close', sql.FLOAT()))
            # Implement the creation
            meta.create_all(create_engine(db_path))
            max_times[symbol] = dt.datetime(2020,1,1)
        # Table already exists, get the last rows datetime info
        else:
            try:
                max_times[symbol] = cx.read_sql(db_path, f'SELECT * FROM {title} ORDER BY Time DESC LIMIT 1').values[0][0]
            except:
                max_times[symbol] = dt.datetime(2020,1,1)
                print(f"{symbol} messed up times. Set to default")
        print(f"Grabbed {symbol} time")    
    print("All symbol times grabbed")
    return max_times

# Used to delete PA data from a specified date to now
def deleteRows(symbol, start_date):
    create_engine(db_path).execute(f"DELETE FROM '{symbol}' WHERE Time > '{start_date}'")
    print("Deleted")

def deleteTable(symbol):
    metadata = MetaData()
    metadata.reflect(bind=main_engine)
    table = metadata.tables[symbol]
    if table is not None:
        metadata.drop_all(main_engine, [table], checkfirst=True)
    print(f"{symbol} Table has been deleted")

# Compares tradeable assets on Bybit with assets in DB, if there are any delisted assets in the DB this deletes the table to save space and speed things up.
def delistCheck():
    qry = f"""SELECT name FROM sqlite_schema WHERE type = 'table'"""
    db_symbols = cx.read_sql(db_path, qry)
    bybit_symbols = bybit.get_assets()

    for symbol in db_symbols.name:
        modify = False
        if symbol[:3] == 'aaa':
            symbol = symbol[3:]
            modify = True
        if symbol in bybit_symbols:
            continue
        else:
            if modify:
                symbol = "aaa" + symbol
            deleteTable(symbol)
            print(f"Deleted {symbol}")
