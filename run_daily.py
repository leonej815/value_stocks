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

    print("Fetching symbols...")
    symbols = get_filtered_symbols()

    print("Running screener...")
    overnight_screener(symbols, db_conn)

    symbols = get_watchlist_symbols(db_conn)

    candle_info = [
            ("6y", "3mo", "3-month"),
            ("3y", "1mo", "monthly"),
            ("2y", "1wk", "weekly"),
            ("1mo", "1d", "daily"),
            ("5d", "30m", "30-minute")
    ]
    
    for period, interval, label in candle_info:
        print(f"Downloading {label} candles...")
        update_candles(symbols, db_conn, period, interval)

    print("Cleaning up database...")
    cleanup_candles(db_conn)
    
if __name__ == "__main__":
    main()