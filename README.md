# Value Stocks
This project is a DASH web app that screens stocks and then creates a watchlist with the result.
The watchlist displays candle charts for different timeframes and some valuation metrics.

## Core Functionality
* run_daily.py which is a data pipepline that is meant to be automatically run nightly, runs the screener using yfinance. The screener yields only "normal" US stocks that are large cap or higher, that have been listed for at least 5 years, are profitable, have a dividend, and are within 5 percents of the 2 year low. The watchlist tickers are stored along with some metrics: dividend yield, free cash flow yield, EV/EBITDA, and quick ratio.
* run_daily.py collects candle data for the stocks that the screener yields. The collected candle data timeframes are 5 year with 3 month candles, 2 year with 1 month candles, 6 month with 1 week candles, 1 month with daily candles, 5 day with 30 minute candles.
* app.py is the web application file. It loads the candle data for all the symbols, and displays the watchlist. The rows are clickable and expand showing metrics and, a candle chart that has buttons for the different time intervals.

## Tech Stack
* Frontend: Dash (Plotly), Dash Bootstrap Components, HTML/CSS
* Backend: Python 3.11
* Database: SQLite3
* Data Source: yfinance API

## Project Structure
Value-Stocks/
├── app.py              # Dash application and UI layout
├── data_manager.py     # SQL logic and data transformation
├── assets/             # Custom CSS
├── data/               # SQLite database storage
└── requirements.txt    # Project dependencies
