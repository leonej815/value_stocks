from datetime import datetime
import time
import pandas as pd
import yfinance as yf
import sqlite3
import os
import random


def _get_db_path():
    if os.getenv("APP_ENV") == "development":
        db_name = "test_stock_data.db"
    else:
        db_name = "stock_data.db"
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, "data", db_name)
    return db_path   


def get_db_conn():
    # create directory in the path if it is missing and connect
    db_path = _get_db_path()
    db_dir = os.path.dirname(db_path)
    if (db_dir != "") and (not os.path.exists(db_dir)):
        os.makedirs(db_dir)
        print(f"Created directory: {db_dir}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # create watchlist table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            ticker TEXT PRIMARY KEY,
            name TEXT,
            exchange TEXT,
            price REAL,
            two_year_low REAL,
            dist_from_low REAL,
            dividend_yield REAL,
            market_cap REAL,
            ev_ebitda REAL,
            quick_ratio REAL,
            fcf_yield REAL,                   
            last_updated TEXT
        )
    """)
    # create table for eod candles
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS eod_candles (
            ticker TEXT,
            timestamp TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            interval TEXT,
            PRIMARY KEY (ticker, timestamp, interval)
        )
    """)
    # create table for intraday candles
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS intraday_candles (
            ticker TEXT,
            timestamp TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            interval TEXT,
            PRIMARY KEY (ticker, timestamp, interval)
        )
    """)
    # Add these inside _db_init after creating the tables
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_eod_ticker ON eod_candles (ticker, timestamp, interval)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_dist ON watchlist (dist_from_low)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_intraday_ticker_time ON intraday_candles (ticker, timestamp, interval)")
    cursor.execute("PRAGMA journal_mode=WAL;")
    conn.commit()
    print(f"Database created at {db_path}")
    return conn


def get_filtered_symbols():
    # read the pipe delimted data on US equities into dataframes
    nasdaq_url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
    other_url = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
    nasdaq_df = pd.read_csv(nasdaq_url, sep="|")
    other_df = pd.read_csv(other_url, sep="|")
    # drop the last row because it"s file creation time
    nasdaq_df = nasdaq_df[:-1]
    other_df = other_df[:-1]
    # remove rows with non-common keywords in security name
    exclude_keywords = [
        "WARRANT", "RIGHT", "UNITS?", "PREFERRED",
        "DEPOSITARY", "DEPOSITORY", "NOTE", "NOTES", "ETF", "TRUST", "DEBENTURE",
        "FUND", "ETN", "WI", "ADRs?", "ADS$", "PLC", "INDEX",
        "BOND", "CONV", "CORP BOND", "DEBT", "INCOME", "ACQUISITION", "BLANK CHECK",
        "CLASS-C", "CL.C", "CLASS-CAPITAL", "CLASS B", "CL B", "CLASS-B", "CL.B",
        "CLASS C", "CL C", "GDR", "PFD", "REIT"
    ]
    pattern = r"\b(?:" + "|".join(exclude_keywords) + r")\b"
    nasdaq_df = nasdaq_df[
        ~nasdaq_df["Security Name"].str.contains(pattern, case=False, na=False)
    ]
    other_df = other_df[
        ~other_df["Security Name"].str.contains(pattern, case=False, na=False)
    ]
    # the data urls contain a "ETF" column where "N" indicates not an etf
    # the data urls also contain a "Test Issue" column where "N" indicates that it"s not a test symbol
    # remove the etf and test symbol rows and store in a series
    # remove NYSE ARCA and NYSE American rows
    nasdaq_stocks = nasdaq_df[(
        nasdaq_df["ETF"] == "N") & 
        (nasdaq_df["Test Issue"] == "N")
        ]["Symbol"]
    other_stocks = other_df[(
        other_df["ETF"] == "N") & 
        (other_df["Test Issue"] == "N") &
        (other_df["Exchange"] == "N")
        ]["NASDAQ Symbol"]
    # combine the nasdaq_stocks and other_stocks series and strip whitespace
    all_tickers = pd.concat([nasdaq_stocks, other_stocks]).astype(str).str.strip()
    # drop all NaN values if any
    all_tickers = all_tickers.dropna()
    # remove all tickers containing a $, ., +, or -
    all_tickers = all_tickers[~all_tickers.str.contains(r"[\$\.\-\+\^_\*]", na=False)]
    # remove tickers that are 5+ characters and end with W, P, or R
    # to remove any warrants, preffered stocks, and rights that slip through
    all_tickers = all_tickers[~((all_tickers.str.len() >= 5) & 
                                all_tickers.str.endswith(("W", "P", "R", "Q", "Z", "F")))]      
    return sorted(all_tickers.unique().tolist())    


def overnight_screener(symbol_list, db_conn):
    candidates = []
    chunk_size = 500  
    for i in range(0, len(symbol_list), chunk_size):
        symbol_list_chunk = symbol_list[i : i + chunk_size]
        fiveyear_candles_dict = _get_fiveyear_candles(symbol_list_chunk)
        for symbol in fiveyear_candles_dict:
            try:
                candle_df = fiveyear_candles_dict[symbol]
                # check if 5 years of data
                if len(candle_df) < 1250: 
                    continue
                # get the last price and 2 year low and check if the
                # distance of the last price from the two year low is <= 5
                two_year_low = candle_df["Low"].tail(504).min()
                current_price = candle_df["Close"].iloc[-1]               
                distance_from_low = ((current_price / two_year_low) - 1) * 100
                if distance_from_low > 5: 
                    continue
                # get more info for further validation        
                ticker = yf.Ticker(symbol)
                info = ticker.info
                time.sleep(random.uniform(2, 5))
                # must be a US company , be at least a large cap (>10 billion market cap)
                # and have a positive dividend yield
                if info.get("country") != "United States":
                    continue
                market_cap = info.get("marketCap", 0)
                if market_cap < 10e9 or info.get("dividendYield", 0) <= 0:
                    continue
                # valuation metrics
                fcf = info.get("freeCashflow")
                if fcf and market_cap:
                    fcf_yield = fcf / market_cap
                else:
                    fcf_yield = 0
                ev_ebitda = info.get("enterpriseToEbitda", 0)
                quick_ratio = info.get("quickRatio", 0)
                if ev_ebitda == 0 or quick_ratio == 0 or fcf_yield == 0:
                    continue
                # exchange
                if info.get("exchange").lower() == "nms":
                    exchange = "nasdaq"
                elif info.get("exchange").lower() == "nyq":
                    exchange = "nyse"
                else:
                    exchange = "n/a"
                # store data in list
                candidates.append({
                    "ticker": symbol,
                    "name": info.get("longName", "n/a"),
                    "exchange": exchange,
                    "price": round(current_price, 2),
                    "two_year_low": round(two_year_low, 2),
                    "dist_from_low": round(distance_from_low, 1),
                    "dividend_yield": info.get("dividendYield", 0),
                    "market_cap": info.get("marketCap", 0),
                    "ev_ebitda": round(ev_ebitda, 1),
                    "quick_ratio": round(quick_ratio, 1),
                    "fcf_yield": round(fcf_yield * 100, 1),
                    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
            except Exception as e:
                print(f"Error skipping {symbol}: {e}")
                continue
        time.sleep(5)
    # replace watchlist table with new data
    if not candidates:
        print("No candidates found matching criteria. Skipping database update.")
        return
    df_candidates = pd.DataFrame(candidates)
    df_candidates.to_sql("watchlist", db_conn, if_exists="replace", index=False)


def get_watchlist_symbols(db_conn):
    # load list of singles from watchlist table
    symbols = pd.read_sql("SELECT ticker FROM watchlist", db_conn)["ticker"].tolist()
    return symbols


def get_watchlist_info(db_conn):
    try:
        df = pd.read_sql("SELECT ticker, name, exchange, price, dist_from_low, dividend_yield, fcf_yield, ev_ebitda, quick_ratio FROM watchlist ORDER BY dist_from_low ASC", db_conn)
        db_conn.close()
        return df.to_dict("records")
    except: return []


def update_candles(tickers, db_conn, period, interval):
    if not tickers: 
        return  
    # donwload candles from yahoo finance
    yf_candle_df = yf.download(tickers, period=period, interval=interval, group_by="ticker")   
    for ticker in tickers:
        try:
            # multiindex safety check
            if len(tickers) > 1:
                if ticker not in yf_candle_df.columns.get_level_values(0):
                    continue
                candle_df = yf_candle_df[ticker].copy()
            else:
                candle_df = yf_candle_df.copy()
            candle_df = candle_df.dropna().reset_index()
            candle_df["ticker"] = ticker
            candle_df.rename(columns={
                "Date": "timestamp", 
                "index": "timestamp",
                "Datetime": "timestamp",
                "date": "timestamp"
                }, inplace=True, errors="ignore")
            # ensure the timestamp is a datetime object
            candle_df["timestamp"] = pd.to_datetime(candle_df["timestamp"])   
            # if the data has timezone info (UTC), convert it to Eastern
            if candle_df["timestamp"].dt.tz is not None:
                candle_df["timestamp"] = candle_df["timestamp"].dt.tz_convert('America/New_York')
            # check if intraday or eod interval
            if interval in ["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"]:
                db_table = "intraday_candles"
                candle_df["timestamp"] = candle_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                db_table = "eod_candles"
                candle_df["timestamp"] = candle_df["timestamp"].dt.strftime("%Y-%m-%d")
            candle_df["interval"] = interval
            candle_df.columns = [c.lower() for c in candle_df.columns]
            # filter to keep only allowed columns
            allowed_columns = ["ticker", "timestamp", "open", "high", "low", "close", "volume", "interval"]
            final_df = candle_df[[col for col in allowed_columns if col in candle_df.columns]].copy()
            # round candle data to four decimal places
            price_cols = ['open', 'high', 'low', 'close']
            for col in price_cols:
                if col in final_df.columns:
                   final_df[col] = final_df[col].round(4) 
            # use a temp table to avoid duplicates
            final_df.to_sql("temp_candles", db_conn, if_exists="replace", index=False)
            db_conn.execute(f"""
                INSERT OR REPLACE INTO {db_table} (ticker, timestamp, open, high, low, close, volume, interval)
                SELECT ticker, timestamp, open, high, low, close, volume, interval FROM temp_candles
            """)
        except Exception as e:
            print(f"{interval} candle update failed for {ticker}: {e}")  
    db_conn.execute("DROP TABLE IF EXISTS temp_candles")
    db_conn.commit()


def cleanup_candles(db_conn):
    # clean eod_candles table
    retention_rules = {
        "1d": "-60 days",
        "1wk": "-360 days",
        "1mo": "-1100 days",
        "3mo": "-2200 days"
    }
    for interval in retention_rules:
        limit = retention_rules[interval]
        try:
            db_conn.execute("""
                DELETE FROM eod_candles 
                WHERE interval = ? AND timestamp < date("now", ?)
            """, (interval, limit))
        except Exception as e:
            print(f"Error cleaning {interval}: {e}")
        db_conn.commit()
    # clean intraday_candles table
    try:
        db_conn.execute("""
            DELETE FROM intraday_candles
            WHERE timestamp < date("now", '-14 days')
        """)
        db_conn.commit()
    except Exception as e:
        print(f"Error cleaning intraday_candles: {e}")   
    try:
        db_conn.execute("VACUUM")
        db_conn.execute("ANALYZE")
        db_conn.commit()
    except Exception as e:
        print(f"Post-cleanup optimization failed: {e}")


def get_chart_data(db_conn):
    # load all candle data into dataframe for tickers in the watchlist
    query_eod = """
        SELECT e.* FROM eod_candles e
        INNER JOIN watchlist w ON e.ticker = w.ticker
        ORDER BY e.timestamp ASC
    """
    query_intraday = """
        SELECT i.* FROM intraday_candles i
        INNER JOIN watchlist w ON i.ticker = w.ticker
        ORDER BY i.timestamp ASC
    """
    df_eod = pd.read_sql(query_eod, db_conn)
    df_intraday = pd.read_sql(query_intraday, db_conn)
    all_data_df = pd.concat([df_eod, df_intraday])
    # chart windows
    chart_candle_amounts = {
        "3mo": 20,
        "1mo": 24,
        "1wk": 26,
        "1d": 30,
        "30m": 65
    }
    # store data for each chart for each symbol in a 2 level dictionary
    candle_data = {}
    for ticker, ticker_df in all_data_df.groupby("ticker"):
        candle_data[ticker] = {}
        for interval, interval_df in ticker_df.groupby("interval"):
            limit = chart_candle_amounts[interval]
            candle_data[ticker][interval] = interval_df.tail(limit)           
    return candle_data


def _get_fiveyear_candles(symbol_list_chunk):
    # get 2 year history of all symbols in chunk
    data = yf.download(
        symbol_list_chunk, 
        period = "5y", 
        group_by = "ticker", 
        threads = False, 
        progress = False
    )     
    # create dictionary and add symbol data
    candle_dict = {}
    for symbol in symbol_list_chunk:
        try:
            # check if the columns are MultiIndex (standard for multi-ticker yf.download)
            if isinstance(data.columns, pd.MultiIndex):
                # check if this specific ticker exists in the returned data
                if symbol in data.columns.get_level_values(0):
                    df = data[symbol].dropna()
                    if not df.empty:
                        candle_dict[symbol] = df
        except Exception as e:
            print(f"Error storing two year candles for {symbol}: {e}")
            continue               
    return candle_dict