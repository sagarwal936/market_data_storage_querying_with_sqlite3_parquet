"""
Unit tests for parquet_storage.py
Tests Parquet dataset creation, partitioning, and querying.
"""
import unittest
import pandas as pd
import pyarrow.parquet as pq
import os
import tempfile
import shutil
from pathlib import Path

# Add parent directory to path to import modules
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from parquet_storage import (
    init_parquet, load_parquet, get_ticker_data_parquet,
    get_avg_daily_volume_parquet, get_top_tickers_by_return_parquet,
    get_daily_trade_summary_parquet, task1_aapl_rolling_average,
    task2_rolling_volatility, get_parquet_size
)
from data_loader import load_and_validate_data


class TestParquetStorage(unittest.TestCase):
    """Test cases for Parquet storage operations."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.parquet_dir = os.path.join(self.test_dir, 'market_data')
        self.market_data_path = os.path.join(self.test_dir, 'market_data_multi.csv')
        self.tickers_path = os.path.join(self.test_dir, 'tickers.csv')
        
        # Create sample market data CSV with multiple tickers and dates
        sample_data = {
            'timestamp': [
                '2025-11-17 09:30:00',
                '2025-11-17 09:31:00',
                '2025-11-17 09:32:00',
                '2025-11-17 09:30:00',
                '2025-11-17 09:31:00',
                '2025-11-18 09:30:00',
                '2025-11-18 09:31:00',
                '2025-11-19 09:30:00',
                '2025-11-20 09:30:00',
                '2025-11-21 09:30:00'
            ],
            'ticker': ['AAPL', 'AAPL', 'AAPL', 'TSLA', 'TSLA', 'AAPL', 'TSLA', 
                      'AAPL', 'AAPL', 'AAPL'],
            'open': [271.45, 269.12, 270.36, 250.00, 251.00, 272.00, 252.00,
                    273.00, 274.00, 275.00],
            'high': [272.07, 269.38, 271.24, 251.00, 252.00, 273.00, 253.00,
                    274.00, 275.00, 276.00],
            'low': [270.77, 269.0, 270.22, 249.00, 250.00, 271.00, 251.00,
                   272.00, 273.00, 274.00],
            'close': [270.88, 269.24, 270.86, 250.50, 251.50, 272.50, 252.50,
                    273.50, 274.50, 275.50],
            'volume': [1416, 3812, 3046, 2000, 2100, 1500, 2200, 1600, 1700, 1800]
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
        
        # Initialize Parquet dataset
        init_parquet(self.parquet_dir, self.market_data_path, self.tickers_path)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.test_dir)
    
    def test_parquet_directory_creation(self):
        """Test that Parquet directory is created."""
        self.assertTrue(os.path.exists(self.parquet_dir))
        self.assertTrue(os.path.isdir(self.parquet_dir))
    
    def test_parquet_partitioning(self):
        """Test that data is partitioned by ticker."""
        # Check that partition directories exist
        aapl_partition = os.path.join(self.parquet_dir, 'ticker=AAPL')
        tsla_partition = os.path.join(self.parquet_dir, 'ticker=TSLA')
        
        self.assertTrue(os.path.exists(aapl_partition))
        self.assertTrue(os.path.exists(tsla_partition))
        
        # Check that partition directories contain parquet files
        aapl_files = [f for f in os.listdir(aapl_partition) if f.endswith('.parquet')]
        tsla_files = [f for f in os.listdir(tsla_partition) if f.endswith('.parquet')]
        
        self.assertGreater(len(aapl_files), 0)
        self.assertGreater(len(tsla_files), 0)
    
    def test_load_parquet(self):
        """Test loading Parquet data into DataFrame."""
        df = load_parquet(self.parquet_dir)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        self.assertIn('ticker', df.columns)
        self.assertIn('timestamp', df.columns)
        self.assertIn('open', df.columns)
        self.assertIn('close', df.columns)
        
        # Check that timestamp is datetime
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['timestamp']))
    
    def test_get_ticker_data_parquet(self):
        """Test retrieving ticker data for date range."""
        df = get_ticker_data_parquet(self.parquet_dir, 'AAPL', '2025-11-17', '2025-11-18')
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        self.assertTrue(all(df['ticker'] == 'AAPL'))
        
        # Check date filtering
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        self.assertTrue(all(df['timestamp'] >= pd.to_datetime('2025-11-17')))
        self.assertTrue(all(df['timestamp'] <= pd.to_datetime('2025-11-18')))
    
    def test_get_avg_daily_volume_parquet(self):
        """Test calculating average daily volume per ticker."""
        df = get_avg_daily_volume_parquet(self.parquet_dir)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        self.assertIn('symbol', df.columns)
        self.assertIn('avg_daily_volume', df.columns)
        
        # Check that all tickers are present
        symbols = set(df['symbol'].unique())
        self.assertIn('AAPL', symbols)
        self.assertIn('TSLA', symbols)
        
        # Check that volumes are positive integers
        self.assertTrue(all(df['avg_daily_volume'] > 0))
        self.assertTrue(all(df['avg_daily_volume'] == df['avg_daily_volume'].astype(int)))
    
    def test_get_top_tickers_by_return_parquet(self):
        """Test identifying top tickers by return."""
        df = get_top_tickers_by_return_parquet(self.parquet_dir, '2025-11-17', '2025-11-18', top_n=2)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertLessEqual(len(df), 2)
        self.assertIn('symbol', df.columns)
        self.assertIn('return_pct', df.columns)
        
        # Check that returns are sorted descending
        if len(df) > 1:
            returns = df['return_pct'].values
            self.assertTrue(all(returns[i] >= returns[i+1] for i in range(len(returns)-1)))
    
    def test_get_daily_trade_summary_parquet(self):
        """Test getting first and last trade prices per day."""
        df = get_daily_trade_summary_parquet(self.parquet_dir)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        self.assertIn('symbol', df.columns)
        self.assertIn('timestamp', df.columns)
        self.assertIn('first_trade_price', df.columns)
        self.assertIn('last_trade_price', df.columns)
        
        # Check that prices are valid
        self.assertTrue(all(df['first_trade_price'] > 0))
        self.assertTrue(all(df['last_trade_price'] > 0))
    
    def test_get_daily_trade_summary_parquet_with_limit(self):
        """Test daily trade summary with limit."""
        df = get_daily_trade_summary_parquet(self.parquet_dir, limit=3)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertLessEqual(len(df), 3)
    
    def test_task1_aapl_rolling_average(self):
        """Test computing rolling average for AAPL."""
        df = task1_aapl_rolling_average(self.parquet_dir, window=5)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        self.assertTrue(all(df['ticker'] == 'AAPL'))
        self.assertIn('close', df.columns)
        self.assertIn('close_5min_ma', df.columns)
        
        # Check that rolling average is computed
        self.assertFalse(df['close_5min_ma'].isnull().all())
    
    def test_task2_rolling_volatility(self):
        """Test computing rolling volatility."""
        df = task2_rolling_volatility(self.parquet_dir, window=5)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        self.assertIn('ticker', df.columns)
        self.assertIn('returns', df.columns)
        self.assertIn('volatility_5d', df.columns)
        
        # Check that volatility is computed (may have NaN for first few rows)
        self.assertFalse(df['volatility_5d'].isnull().all())
    
    def test_get_parquet_size(self):
        """Test calculating Parquet directory size."""
        size = get_parquet_size(self.parquet_dir)
        
        self.assertGreater(size, 0)
        self.assertIsInstance(size, (int, float))
    
    def test_partition_pruning(self):
        """Test that partition pruning works correctly."""
        # Query for AAPL should only read AAPL partition
        aapl_df = get_ticker_data_parquet(self.parquet_dir, 'AAPL', '2025-11-17', '2025-11-18')
        
        # Query for TSLA should only read TSLA partition
        tsla_df = get_ticker_data_parquet(self.parquet_dir, 'TSLA', '2025-11-17', '2025-11-18')
        
        # Both should work and return correct data
        self.assertGreater(len(aapl_df), 0)
        self.assertGreater(len(tsla_df), 0)
        self.assertTrue(all(aapl_df['ticker'] == 'AAPL'))
        self.assertTrue(all(tsla_df['ticker'] == 'TSLA'))
    
    def test_data_integrity(self):
        """Test that data integrity is maintained in Parquet."""
        original_df = pd.read_csv(self.market_data_path)
        parquet_df = load_parquet(self.parquet_dir)
        
        # Check that all rows are preserved
        self.assertEqual(len(original_df), len(parquet_df))
        
        # Check that all tickers are present
        original_tickers = set(original_df['ticker'].unique())
        parquet_tickers = set(parquet_df['ticker'].unique())
        self.assertEqual(original_tickers, parquet_tickers)


if __name__ == '__main__':
    unittest.main()

