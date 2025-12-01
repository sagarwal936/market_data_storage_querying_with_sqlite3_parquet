import sqlite3
import pandas as pd
import os
from data_loader import load_and_validate_data

DB_NAME = 'market_data.db'
TICKERS_FILE = 'tickers.csv'
PRICES_FILE = 'market_data_multi.csv'

#########

def init_db(connection):
    cursor = connection.cursor()
    # Enable foreign key constraints
    cursor.execute("PRAGMA foreign_keys = ON")
    schema_sql = """
    CREATE TABLE IF NOT EXISTS tickers (
        ticker_id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL UNIQUE,
        name TEXT,
        exchange TEXT
    );

    CREATE TABLE IF NOT EXISTS prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        ticker_id INTEGER NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id)
    );
    """
    cursor.executescript(schema_sql)
    connection.commit()
    print("Database schema initialized.")

def import_tickers(connection, csv_path):
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found.")
        return

    tickers_df = pd.read_csv(csv_path)
    
    try:
        tickers_df.to_sql('tickers', connection, if_exists='replace', index=False)
        print(f"Successfully inserted tickers from {csv_path}.")
        
    except sqlite3.IntegrityError as e:
        print(f"Notice: Tickers could not be inserted directly: {e}")
        print("Tip: This usually means these tickers already exist in the DB.")
    except Exception as e:
        print(f"An unexpected error occurred while importing tickers: {e}")

def get_symbol_map(connection):
    map_df = pd.read_sql("SELECT symbol, ticker_id FROM tickers", connection)
    return dict(zip(map_df['symbol'], map_df['ticker_id']))

def import_prices(connection, prices_path, tickers_path):
    symbol_to_id = get_symbol_map(connection)
    
    if not symbol_to_id:
        print("Error: No tickers found in database. Cannot import prices.")
        return

    print("Validating and cleaning price data...")
    try:
        clean_df = load_and_validate_data(prices_path, tickers_path)
    except FileNotFoundError as e:
        print(f"Error loading data files: {e}")
        return

    clean_df['ticker_id'] = clean_df['ticker'].map(symbol_to_id)

    missing_ids = clean_df[clean_df['ticker_id'].isna()]
    if not missing_ids.empty:
        print(f"Warning: Dropping {len(missing_ids)} rows because their ticker symbol is not in the tickers table.")
        clean_df = clean_df.dropna(subset=['ticker_id'])

    prices_to_db = clean_df[['timestamp', 'ticker_id', 'open', 'high', 'low', 'close', 'volume']]

    prices_to_db.to_sql('prices', connection, if_exists='replace', index=False)
    print(f"Successfully loaded {len(prices_to_db)} price records into SQLite.")


######################################### Data Querying Functions #########################################

def get_ticker_data(conn, ticker, start_date, end_date):
    query = """
    SELECT t.symbol, p.timestamp, p.open, p.high, p.low, p.close, p.volume
    FROM prices p
    JOIN tickers t ON p.ticker_id = t.ticker_id
    WHERE t.symbol = ? 
      AND p.timestamp BETWEEN ? AND ?
    ORDER BY p.timestamp ASC
    """
    return pd.read_sql(query, conn, params=(ticker, start_date, end_date))

def get_avg_daily_volume(conn):
    query = """
    SELECT 
        t.symbol, 
        CAST(AVG(p.volume) AS INTEGER) as avg_daily_volume
    FROM prices p
    JOIN tickers t ON p.ticker_id = t.ticker_id
    GROUP BY t.symbol
    ORDER BY avg_daily_volume DESC
    """
    return pd.read_sql(query, conn)

def get_top_tickers_by_return(conn, start_date, end_date, top_n=3):
    query = """
    WITH period_boundaries AS (
        SELECT 
            ticker_id, 
            MIN(timestamp) as first_date, 
            MAX(timestamp) as last_date
        FROM prices
        WHERE timestamp BETWEEN ? AND ?
        GROUP BY ticker_id
    ),
    start_prices AS (
        SELECT p.ticker_id, p.open as start_price
        FROM prices p
        JOIN period_boundaries pb ON p.ticker_id = pb.ticker_id AND p.timestamp = pb.first_date
    ),
    end_prices AS (
        SELECT p.ticker_id, p.close as end_price
        FROM prices p
        JOIN period_boundaries pb ON p.ticker_id = pb.ticker_id AND p.timestamp = pb.last_date
    )
    SELECT 
        t.symbol,
        ROUND(((e.end_price - s.start_price) / s.start_price) * 100, 2) as return_pct
    FROM tickers t
    JOIN start_prices s ON t.ticker_id = s.ticker_id
    JOIN end_prices e ON t.ticker_id = e.ticker_id
    ORDER BY return_pct DESC
    LIMIT ?
    """
    return pd.read_sql(query, conn, params=(start_date, end_date, top_n))

def get_daily_trade_summary(conn, limit=None):
    query = """
    WITH daily_boundaries AS (
        SELECT 
            ticker_id, 
            DATE(timestamp) as trade_date,
            MIN(timestamp) as start_time, 
            MAX(timestamp) as end_time
        FROM prices
        GROUP BY ticker_id, DATE(timestamp)
    )
    SELECT 
        t.symbol, 
        db.trade_date as timestamp, 
        p_start.open as first_trade_price, 
        p_end.close as last_trade_price
    FROM daily_boundaries db
    JOIN tickers t ON db.ticker_id = t.ticker_id
    JOIN prices p_start ON db.ticker_id = p_start.ticker_id AND p_start.timestamp = db.start_time
    JOIN prices p_end ON db.ticker_id = p_end.ticker_id AND p_end.timestamp = db.end_time
    ORDER BY db.trade_date DESC, t.symbol ASC
    """
    params = ()
    if limit:
        query += " LIMIT ?"
        params = (limit,)
        
    return pd.read_sql(query, conn, params=params)

def get_all_market_data(conn, ticker, start_date, end_date):
    return {
        "ticker_history": get_ticker_data(conn, ticker, start_date, end_date),
        "volume_summary": get_avg_daily_volume(conn),
        "top_returns": get_top_tickers_by_return(conn, start_date, end_date),
        "daily_summary": get_daily_trade_summary(conn)
    }


########################## Main Execution Flow ##########################

def main():
    try:
        conn = sqlite3.connect(DB_NAME)
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return

    try:
        init_db(conn)
        import_tickers(conn, TICKERS_FILE)
        import_prices(conn, PRICES_FILE, TICKERS_FILE)
        dfs = get_all_market_data(conn, 'AAPL', '2025-11-17', '2025-11-22')
        for i in dfs:
            print(f"\n--- {i} ---")
            print(dfs[i].head())
    finally:
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()