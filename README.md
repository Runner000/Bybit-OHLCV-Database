# Bybit-OHLCV-Database
Python program that grabs OHLCV market data from Bybit servers for all tradeable perpetual assets. 
1. Grabs all of the data and reformats it for compatibility with a SQLite DB.
2. Mass imports market data to your desired DB  
&nbsp;&nbsp;&nbsp;&nbsp;i. If there is a new asset the program creates a new table in the DB.
3. I have also included a few files that are able to export data from the DB and use it to analyze various market patterns and find any tradable correlations or edges.  
&nbsp;&nbsp;&nbsp;&nbsp;i. You can also forward your research and results to a discord server which i have included.
5. These processes can be automated via a batch file.

# HOW TO:
1. Install requirements
3. Bybit Setup  
&nbsp;&nbsp;&nbsp;&nbsp;i. This includes creating an account and having API access  
&nbsp;&nbsp;&nbsp;&nbsp;ii. You need to copy your API details into the login file.
5. Figure out which assets you want to download from Bybit  
&nbsp;&nbsp;&nbsp;&nbsp;i. Currently downloads all tradeable perpetual assets, primarily the 5m OHLCV data (this can be changed to any timeframe)
6. Once your DB is established you can export OHLCV data and analyze it however you want or use what I have created.  
&nbsp;&nbsp;&nbsp;&nbsp;i. Currently analyzes BTC, ETH, and ALL ASSETS in the DB  
&nbsp;&nbsp;&nbsp;&nbsp;ii. Data is collected and sent to a Discord server. Feel free to adapt this part, you can also send plots.

