"""
Unit tests for data_loader.py
Tests data loading, validation, and normalization.
"""
import unittest
import pandas as pd
import os
import tempfile
import shutil
from pathlib import Path

# Add parent directory to path to import modules
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_loader import load_and_validate_data


class TestDataLoader(unittest.TestCase):
    """Test cases for data loading and validation."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.market_data_path = os.path.join(self.test_dir, 'market_data_multi.csv')
        self.tickers_path = os.path.join(self.test_dir, 'tickers.csv')
        
        # Create sample market data CSV
        sample_data = {
            'timestamp': [
                '2025-11-17 09:30:00',
                '2025-11-17 09:31:00',
                '2025-11-17 09:32:00',
                '2025-11-17 09:30:00',
                '2025-11-17 09:31:00'
            ],
            'ticker': ['AAPL', 'AAPL', 'AAPL', 'TSLA', 'TSLA'],
            'open': [271.45, 269.12, 270.36, 250.00, 251.00],
            'high': [272.07, 269.38, 271.24, 251.00, 252.00],
            'low': [270.77, 269.0, 270.22, 249.00, 250.00],
            'close': [270.88, 269.24, 270.86, 250.50, 251.50],
            'volume': [1416, 3812, 3046, 2000, 2100]
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
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.test_dir)
    
    def test_load_valid_data(self):
        """Test loading valid market data."""
        df = load_and_validate_data(self.market_data_path, self.tickers_path)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        self.assertIn('ticker', df.columns)
        self.assertIn('timestamp', df.columns)
        self.assertIn('open', df.columns)
        self.assertIn('close', df.columns)
    
    def test_validate_tickers_present(self):
        """Test that all tickers from reference file are present."""
        df = load_and_validate_data(self.market_data_path, self.tickers_path)
        
        # Check that all tickers from tickers.csv are in the data
        tickers_ref = pd.read_csv(self.tickers_path)
        expected_tickers = set(tickers_ref['symbol'].unique())
        actual_tickers = set(df['ticker'].unique())
        
        self.assertTrue(expected_tickers.issubset(actual_tickers))
    
    def test_missing_ticker_raises_error(self):
        """Test that missing tickers raise ValueError."""
        # Create tickers CSV with a ticker not in market data
        tickers_data = {
            'ticker_id': [1, 2, 3],
            'symbol': ['AAPL', 'TSLA', 'MSFT'],  # MSFT not in market data
            'name': ['Apple Inc.', 'Tesla Inc.', 'Microsoft Corp.'],
            'exchange': ['NASDAQ', 'NASDAQ', 'NASDAQ']
        }
        tickers_df = pd.DataFrame(tickers_data)
        tickers_path = os.path.join(self.test_dir, 'tickers_missing.csv')
        tickers_df.to_csv(tickers_path, index=False)
        
        with self.assertRaises(ValueError) as context:
            load_and_validate_data(self.market_data_path, tickers_path)
        
        self.assertIn('missing', str(context.exception).lower())
    
    def test_missing_timestamps_raises_error(self):
        """Test that missing timestamps raise ValueError."""
        # Create data with missing timestamps
        sample_data = {
            'timestamp': [
                '2025-11-17 09:30:00',
                None,  # Missing timestamp
                '2025-11-17 09:32:00'
            ],
            'ticker': ['AAPL', 'AAPL', 'AAPL'],
            'open': [271.45, 269.12, 270.36],
            'high': [272.07, 269.38, 271.24],
            'low': [270.77, 269.0, 270.22],
            'close': [270.88, 269.24, 270.86],
            'volume': [1416, 3812, 3046]
        }
        df = pd.DataFrame(sample_data)
        invalid_path = os.path.join(self.test_dir, 'invalid_data.csv')
        df.to_csv(invalid_path, index=False)
        
        with self.assertRaises(ValueError) as context:
            load_and_validate_data(invalid_path, self.tickers_path)
        
        self.assertIn('timestamp', str(context.exception).lower())
    
    def test_missing_prices_raises_error(self):
        """Test that missing price values raise ValueError."""
        # Create data with missing prices
        sample_data = {
            'timestamp': [
                '2025-11-17 09:30:00',
                '2025-11-17 09:31:00',
                '2025-11-17 09:32:00'
            ],
            'ticker': ['AAPL', 'AAPL', 'AAPL'],
            'open': [271.45, None, 270.36],  # Missing price
            'high': [272.07, 269.38, 271.24],
            'low': [270.77, 269.0, 270.22],
            'close': [270.88, None, 270.86],  # Missing price
            'volume': [1416, 3812, 3046]
        }
        df = pd.DataFrame(sample_data)
        invalid_path = os.path.join(self.test_dir, 'invalid_prices.csv')
        df.to_csv(invalid_path, index=False)
        
        with self.assertRaises(ValueError) as context:
            load_and_validate_data(invalid_path, self.tickers_path)
        
        self.assertIn('price', str(context.exception).lower())
    
    def test_normalize_column_names(self):
        """Test that column names are normalized to lowercase."""
        # Create data with uppercase column names
        sample_data = {
            'Timestamp': ['2025-11-17 09:30:00', '2025-11-17 09:31:00'],
            'Ticker': ['AAPL', 'AAPL'],
            'Open': [271.45, 269.12],
            'High': [272.07, 269.38],
            'Low': [270.77, 269.0],
            'Close': [270.88, 269.24],
            'Volume': [1416, 3812]
        }
        df = pd.DataFrame(sample_data)
        uppercase_path = os.path.join(self.test_dir, 'uppercase_data.csv')
        df.to_csv(uppercase_path, index=False)
        
        result_df = load_and_validate_data(uppercase_path, self.tickers_path)
        
        # Check that columns are lowercase (data_loader normalizes column names)
        # Note: The function normalizes to lowercase, so uppercase columns should work
        self.assertIn('timestamp', result_df.columns)
        self.assertIn('ticker', result_df.columns)
        self.assertIn('open', result_df.columns)
        # Verify uppercase columns are not present
        self.assertNotIn('Timestamp', result_df.columns)
        self.assertNotIn('Ticker', result_df.columns)
    
    def test_datetime_conversion(self):
        """Test that timestamps are converted to datetime."""
        df = load_and_validate_data(self.market_data_path, self.tickers_path)
        
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['timestamp']))
    
    def test_file_not_found_error(self):
        """Test that missing files raise FileNotFoundError."""
        with self.assertRaises(FileNotFoundError):
            load_and_validate_data('nonexistent.csv', self.tickers_path)
        
        with self.assertRaises(FileNotFoundError):
            load_and_validate_data(self.market_data_path, 'nonexistent.csv')


if __name__ == '__main__':
    unittest.main()

