"""
Unit tests for sqlite_storage.py
Tests database schema creation, data insertion, and SQL queries.
"""
import unittest
import sqlite3
import pandas as pd
import os
import tempfile
import shutil
from pathlib import Path

# Add parent directory to path to import modules
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlite_storage import (
    init_db, import_tickers, import_prices,
    get_ticker_data, get_avg_daily_volume,
    get_top_tickers_by_return, get_daily_trade_summary
)
from data_loader import load_and_validate_data


class TestSQLiteStorage(unittest.TestCase):
    """Test cases for SQLite storage operations."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, 'test_market_data.db')
        self.market_data_path = os.path.join(self.test_dir, 'market_data_multi.csv')
        self.tickers_path = os.path.join(self.test_dir, 'tickers.csv')
        
        # Create sample market data CSV
        sample_data = {
            'timestamp': [
                '2025-11-17 09:30:00',
                '2025-11-17 09:31:00',
                '2025-11-17 09:32:00',
                '2025-11-17 09:30:00',
                '2025-11-17 09:31:00',
                '2025-11-18 09:30:00',
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
        df.to_csv(self.market_data_path, index=False)
        
        # Create sample tickers CSV
        tickers_data = {
            'ticker_id': [1, 2],
            'symbol': ['AAPL', 'TSLA'],
            'name': ['Apple Inc.', 'Tesla Inc.'],
            'exchange': ['NASDAQ', 'NASDAQ']
        }
        tickers_df = pd.DataFrame(tickers_data)
        tickers_df.to_csv(self.tickers_path, index=False)
        
        # Initialize database
        self.conn = sqlite3.connect(self.db_path)
        init_db(self.conn)
        import_tickers(self.conn, self.tickers_path)
        import_prices(self.conn, self.market_data_path, self.tickers_path)
    
    def tearDown(self):
        """Clean up test fixtures."""
        if self.conn:
            self.conn.close()
        shutil.rmtree(self.test_dir)
    
    def test_schema_creation(self):
        """Test that database schema is created correctly."""
        cursor = self.conn.cursor()
        
        # Check tickers table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tickers'")
        self.assertIsNotNone(cursor.fetchone())
        
        # Check prices table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='prices'")
        self.assertIsNotNone(cursor.fetchone())
        
        # Check tickers table structure
        cursor.execute("PRAGMA table_info(tickers)")
        tickers_columns = [row[1] for row in cursor.fetchall()]
        self.assertIn('ticker_id', tickers_columns)
        self.assertIn('symbol', tickers_columns)
        
        # Check prices table structure
        cursor.execute("PRAGMA table_info(prices)")
        prices_columns = [row[1] for row in cursor.fetchall()]
        self.assertIn('ticker_id', prices_columns)
        self.assertIn('timestamp', prices_columns)
        self.assertIn('open', prices_columns)
        self.assertIn('close', prices_columns)
    
    def test_ticker_import(self):
        """Test that tickers are imported correctly."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM tickers")
        count = cursor.fetchone()[0]
        
        self.assertGreater(count, 0)
        
        # Check specific tickers
        cursor.execute("SELECT symbol FROM tickers")
        symbols = [row[0] for row in cursor.fetchall()]
        self.assertIn('AAPL', symbols)
        self.assertIn('TSLA', symbols)
    
    def test_price_import(self):
        """Test that prices are imported correctly."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM prices")
        count = cursor.fetchone()[0]
        
        self.assertGreater(count, 0)
        self.assertEqual(count, 7)  # Should match sample data
    
    def test_get_ticker_data(self):
        """Test retrieving ticker data for date range."""
        df = get_ticker_data(self.conn, 'AAPL', '2025-11-17', '2025-11-18')
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        self.assertTrue(all(df['symbol'] == 'AAPL'))
        
        # Check date filtering
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        self.assertTrue(all(df['timestamp'] >= pd.to_datetime('2025-11-17')))
        self.assertTrue(all(df['timestamp'] <= pd.to_datetime('2025-11-18')))
    
    def test_get_avg_daily_volume(self):
        """Test calculating average daily volume per ticker."""
        df = get_avg_daily_volume(self.conn)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        self.assertIn('symbol', df.columns)
        self.assertIn('avg_daily_volume', df.columns)
        
        # Check that all tickers are present
        symbols = set(df['symbol'].unique())
        self.assertIn('AAPL', symbols)
        self.assertIn('TSLA', symbols)
        
        # Check that volumes are positive
        self.assertTrue(all(df['avg_daily_volume'] > 0))
    
    def test_get_top_tickers_by_return(self):
        """Test identifying top tickers by return."""
        df = get_top_tickers_by_return(self.conn, '2025-11-17', '2025-11-18', top_n=2)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertLessEqual(len(df), 2)
        self.assertIn('symbol', df.columns)
        self.assertIn('return_pct', df.columns)
        
        # Check that returns are sorted descending
        if len(df) > 1:
            returns = df['return_pct'].values
            self.assertTrue(all(returns[i] >= returns[i+1] for i in range(len(returns)-1)))
    
    def test_get_daily_trade_summary(self):
        """Test getting first and last trade prices per day."""
        df = get_daily_trade_summary(self.conn)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        self.assertIn('symbol', df.columns)
        self.assertIn('timestamp', df.columns)
        self.assertIn('first_trade_price', df.columns)
        self.assertIn('last_trade_price', df.columns)
        
        # Check that prices are valid
        self.assertTrue(all(df['first_trade_price'] > 0))
        self.assertTrue(all(df['last_trade_price'] > 0))
    
    def test_get_daily_trade_summary_with_limit(self):
        """Test daily trade summary with limit."""
        df = get_daily_trade_summary(self.conn, limit=3)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertLessEqual(len(df), 3)
    
    def test_foreign_key_constraint(self):
        """Test that foreign key constraint is enforced."""
        cursor = self.conn.cursor()
        # Ensure foreign keys are enabled
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Try to insert price with invalid ticker_id
        with self.assertRaises(sqlite3.IntegrityError):
            cursor.execute("""
                INSERT INTO prices (timestamp, ticker_id, open, high, low, close, volume)
                VALUES ('2025-11-17 09:30:00', 999, 100.0, 101.0, 99.0, 100.5, 1000)
            """)
            self.conn.commit()
    
    def test_data_integrity(self):
        """Test that data integrity is maintained."""
        cursor = self.conn.cursor()
        
        # Check that all prices have valid ticker_id
        cursor.execute("""
            SELECT COUNT(*) FROM prices p
            LEFT JOIN tickers t ON p.ticker_id = t.ticker_id
            WHERE t.ticker_id IS NULL
        """)
        orphan_count = cursor.fetchone()[0]
        self.assertEqual(orphan_count, 0)


if __name__ == '__main__':
    unittest.main()

