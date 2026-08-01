from dotenv import load_dotenv
load_dotenv()
from data_manager import(
    get_db_conn, 
    get_filtered_symbols, 
    overnight_screener, 
    update_candles, 
    get_watchlist_symbols, 
    cleanup_candles
)


def main():
    db_conn = get_db_conn()

    # get list of "normal" stock symbols
    print("Fetching symbols...")
    symbols = get_filtered_symbols()

    # run screener and store symbols that passed the screener in the watchlist
    print("Running screener...")
    overnight_screener(symbols, db_conn)

    # load current watchlist symbols
    symbols = get_watchlist_symbols(db_conn)

    # timeframes and candle intervals to retrieve candles for
    candle_info = [
            ("5y", "1mo", "monthly"),
            ("1y", "1wk", "weekly"),
            ("1mo", "1d", "daily"),
            ("5d", "30m", "30-minute")
    ]

    # download candles and save to database
    for period, interval, label in candle_info:
        print(f"Downloading {label} candles...")
        update_candles(symbols, db_conn, period, interval)

    # delete old data from database
    print("Cleaning up database...")
    cleanup_candles(db_conn)
    
if __name__ == "__main__":
    main()