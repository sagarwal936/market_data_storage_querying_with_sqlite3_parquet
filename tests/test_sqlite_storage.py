"""
Unit tests for sqlite_storage.py
Tests database schema creation, data insertion, and SQL queries.
Converted to pytest.
"""
import pytest
import sqlite3
import pandas as pd
import os
import sys
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlite_storage import (
    init_db, import_tickers, import_prices,
    get_ticker_data, get_avg_daily_volume,
    get_top_tickers_by_return, get_daily_trade_summary
)

# Fixtures

@pytest.fixture
def db_environment(tmp_path):
    """
    Creates a temporary environment with a populated SQLite database
    and sample CSV files. Returns a dictionary with the connection and paths.
    """
    # Define paths using pathlib (tmp_path is a Path object)
    db_path = tmp_path / 'test_market_data.db'
    market_data_path = tmp_path / 'market_data_multi.csv'
    tickers_path = tmp_path / 'tickers.csv'

    # Create sample market data CSV
    sample_data = {
        'timestamp': [
            '2025-11-17 09:30:00', '2025-11-17 09:31:00', '2025-11-17 09:32:00',
            '2025-11-17 09:30:00', '2025-11-17 09:31:00', '2025-11-18 09:30:00',
            '2025-11-18 09:31:00'
        ],
        'ticker': ['AAPL', 'AAPL', 'AAPL', 'TSLA', 'TSLA', 'AAPL', 'TSLA'],
        'open': [271.45, 269.12, 270.36, 250.00, 251.00, 272.00, 252.00],
        'high': [272.07, 269.38, 271.24, 251.00, 252.00, 273.00, 253.00],
        'low': [270.77, 269.0, 270.22, 249.00, 250.00, 271.00, 251.00],
        'close': [270.88, 269.24, 270.86, 250.50, 251.50, 272.50, 252.50],
        'volume': [1416, 3812, 3046, 2000, 2100, 1500, 2200]
    }
    df = pd.DataFrame(sample_data)
    df.to_csv(market_data_path, index=False)

    # Create sample tickers CSV
    tickers_data = {
        'ticker_id': [1, 2],
        'symbol': ['AAPL', 'TSLA'],
        'name': ['Apple Inc.', 'Tesla Inc.'],
        'exchange': ['NASDAQ', 'NASDAQ']
    }
    tickers_df = pd.DataFrame(tickers_data)
    tickers_df.to_csv(tickers_path, index=False)

    # Initialize database
    conn = sqlite3.connect(str(db_path))
    init_db(conn)
    
    # Import data - pass paths as strings if the implementation expects strings
    import_tickers(conn, str(tickers_path))
    import_prices(conn, str(market_data_path), str(tickers_path))

    # Yield resources to the test
    yield {
        'conn': conn,
        'db_path': str(db_path),
        'market_data_path': str(market_data_path),
        'tickers_path': str(tickers_path)
    }

    # Teardown: Close connection
    conn.close()


# Tests

def test_schema_creation(db_environment):
    """Test that database schema is created correctly."""
    conn = db_environment['conn']
    cursor = conn.cursor()
    
    # Check tickers table
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tickers'")
    assert cursor.fetchone() is not None
    
    # Check prices table
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='prices'")
    assert cursor.fetchone() is not None
    
    # Check tickers table structure
    cursor.execute("PRAGMA table_info(tickers)")
    tickers_columns = [row[1] for row in cursor.fetchall()]
    assert 'ticker_id' in tickers_columns
    assert 'symbol' in tickers_columns
    
    # Check prices table structure
    cursor.execute("PRAGMA table_info(prices)")
    prices_columns = [row[1] for row in cursor.fetchall()]
    assert 'ticker_id' in prices_columns
    assert 'timestamp' in prices_columns
    assert 'open' in prices_columns
    assert 'close' in prices_columns

def test_ticker_import(db_environment):
    """Test that tickers are imported correctly."""
    conn = db_environment['conn']
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM tickers")
    count = cursor.fetchone()[0]
    assert count > 0
    
    # Check specific tickers
    cursor.execute("SELECT symbol FROM tickers")
    symbols = [row[0] for row in cursor.fetchall()]
    assert 'AAPL' in symbols
    assert 'TSLA' in symbols

def test_price_import(db_environment):
    """Test that prices are imported correctly."""
    conn = db_environment['conn']
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM prices")
    count = cursor.fetchone()[0]
    
    assert count > 0
    assert count == 7  # Should match sample data

def test_get_ticker_data(db_environment):
    """Test retrieving ticker data for date range."""
    conn = db_environment['conn']
    df = get_ticker_data(conn, 'AAPL', '2025-11-17', '2025-11-18')
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert all(df['symbol'] == 'AAPL')
    
    # Check date filtering
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    assert all(df['timestamp'] >= pd.to_datetime('2025-11-17'))
    assert all(df['timestamp'] <= pd.to_datetime('2025-11-18'))

def test_get_avg_daily_volume(db_environment):
    """Test calculating average daily volume per ticker."""
    conn = db_environment['conn']
    df = get_avg_daily_volume(conn)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert 'symbol' in df.columns
    assert 'avg_daily_volume' in df.columns
    
    # Check that all tickers are present
    symbols = set(df['symbol'].unique())
    assert 'AAPL' in symbols
    assert 'TSLA' in symbols
    
    # Check that volumes are positive
    assert all(df['avg_daily_volume'] > 0)

def test_get_top_tickers_by_return(db_environment):
    """Test identifying top tickers by return."""
    conn = db_environment['conn']
    df = get_top_tickers_by_return(conn, '2025-11-17', '2025-11-18', top_n=2)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) <= 2
    assert 'symbol' in df.columns
    assert 'return_pct' in df.columns
    
    # Check that returns are sorted descending
    if len(df) > 1:
        returns = df['return_pct'].values
        assert all(returns[i] >= returns[i+1] for i in range(len(returns)-1))

def test_get_daily_trade_summary(db_environment):
    """Test getting first and last trade prices per day."""
    conn = db_environment['conn']
    df = get_daily_trade_summary(conn)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert 'symbol' in df.columns
    assert 'timestamp' in df.columns
    assert 'first_trade_price' in df.columns
    assert 'last_trade_price' in df.columns
    
    # Check that prices are valid
    assert all(df['first_trade_price'] > 0)
    assert all(df['last_trade_price'] > 0)

def test_get_daily_trade_summary_with_limit(db_environment):
    """Test daily trade summary with limit."""
    conn = db_environment['conn']
    df = get_daily_trade_summary(conn, limit=3)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) <= 3

def test_data_integrity(db_environment):
    """Test that data integrity is maintained."""
    conn = db_environment['conn']
    cursor = conn.cursor()
    
    # Check that all prices have valid ticker_id
    cursor.execute("""
        SELECT COUNT(*) FROM prices p
        LEFT JOIN tickers t ON p.ticker_id = t.ticker_id
        WHERE t.ticker_id IS NULL
    """)
    orphan_count = cursor.fetchone()[0]
    assert orphan_count == 0