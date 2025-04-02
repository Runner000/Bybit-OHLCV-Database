import connectorx as cx, Update_DB.github.bybit_funcs as bybit
import datetime as dt
from sqlalchemy import create_engine, MetaData, Table, Column, inspect
import sqlalchemy as sql
from parameters import db_path

main_engine = create_engine(db_path)
   
def bulk_db_insert(df_list):
    """
    Inserts multiple DataFrames into a database table.
    Parameters:
    df_list (list): A list of dictionaries where each dictionary contains a symbol as the key 
                    and a DataFrame as the value.
    This function iterates through the list of DataFrames, checks the symbol for each DataFrame,
    and inserts the data into the corresponding table in the database. If the symbol starts with 
    a non-alphabetic character, it prefixes the symbol with 'aaa'. It uses the SQLAlchemy engine 
    to perform the insertion and prints the number of new rows imported for each symbol.
    """
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
    """
    Retrieves the last time associated with each symbol table in the database.
    Parameters:
    symbols (list): A list of symbol names to check in the database.
    Returns:
    dict: A dictionary where keys are symbol names and values are the last recorded time 
          for each symbol.
    This function first checks if the last times for the first and last symbol tables are the same.
    If they are, it quickly assigns the same time to all symbols. If not, it checks each symbol 
    individually, creating a new table if it doesn't exist and retrieving the last time if it does.
    """
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
    """
    Deletes rows from a specified symbol table in the database based on a date condition.
    Parameters:
    symbol (str): The name of the symbol table from which to delete rows.
    start_date (str): The date from which to delete rows (all rows with a 'Time' greater than this date will be deleted).
    This function executes a DELETE SQL command to remove all rows from the specified symbol table 
    where the 'Time' column is greater than the provided start_date. After executing the command, 
    it prints a confirmation message indicating that the rows have been deleted.
    """
    create_engine(db_path).execute(f"DELETE FROM '{symbol}' WHERE Time > '{start_date}'")
    print("Deleted")

def deleteTable(symbol):
    """
    Deletes a specified table from the database.
    Parameters:
    symbol (str): The name of the table to be deleted.
    This function reflects the current database schema to check if the specified table exists. 
    If the table is found, it drops the table from the database. A confirmation message is printed 
    indicating that the table has been deleted.
    """
    metadata = MetaData()
    metadata.reflect(bind=main_engine)
    table = metadata.tables[symbol]
    if table is not None:
        metadata.drop_all(main_engine, [table], checkfirst=True)
    print(f"{symbol} Table has been deleted")

# Compares tradeable assets on Bybit with assets in DB, if there are any delisted assets in the DB this deletes the table to save space and speed things up.
def delistCheck():
    """
    Compares tradeable assets on Bybit with assets in the database and deletes any delisted assets.
    This function retrieves the list of tables (symbols) from the database and compares them with 
    the current tradeable assets available on Bybit. If any symbols in the database are not found 
    in the Bybit assets list, the corresponding table is deleted to save space and improve performance. 
    A message is printed for each deleted table.
    """
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
