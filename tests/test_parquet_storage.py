import pytest
import pandas as pd
import os
import sys
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from parquet_storage import (
    init_parquet, load_parquet, get_ticker_data_parquet,
    get_avg_daily_volume_parquet, get_top_tickers_by_return_parquet,
    get_daily_trade_summary_parquet, task1_aapl_rolling_average,
    task2_rolling_volatility, get_parquet_size
)

# Fixtures

@pytest.fixture
def parquet_dataset(tmp_path):
    """
    Creates a temporary environment with sample CSV data and initializes 
    the Parquet store. Returns a dictionary containing paths to the 
    resources.
    """
    # Define paths
    parquet_dir = tmp_path / 'market_data'
    market_data_path = tmp_path / 'market_data_multi.csv'
    tickers_path = tmp_path / 'tickers.csv'

    # Create sample market data CSV
    sample_data = {
        'timestamp': [
            '2025-11-17 09:30:00', '2025-11-17 09:31:00', '2025-11-17 09:32:00',
            '2025-11-17 09:30:00', '2025-11-17 09:31:00', '2025-11-18 09:30:00',
            '2025-11-18 09:31:00', '2025-11-19 09:30:00', '2025-11-20 09:30:00',
            '2025-11-21 09:30:00'
        ],
        'ticker': [
            'AAPL', 'AAPL', 'AAPL', 'TSLA', 'TSLA', 'AAPL', 'TSLA', 
            'AAPL', 'AAPL', 'AAPL'
        ],
        'open': [271.45, 269.12, 270.36, 250.00, 251.00, 272.00, 252.00, 273.00, 274.00, 275.00],
        'high': [272.07, 269.38, 271.24, 251.00, 252.00, 273.00, 253.00, 274.00, 275.00, 276.00],
        'low': [270.77, 269.0, 270.22, 249.00, 250.00, 271.00, 251.00, 272.00, 273.00, 274.00],
        'close': [270.88, 269.24, 270.86, 250.50, 251.50, 272.50, 252.50, 273.50, 274.50, 275.50],
        'volume': [1416, 3812, 3046, 2000, 2100, 1500, 2200, 1600, 1700, 1800]
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

    # Initialize Parquet dataset
    # Converting Path objects to strings as underlying library likely expects strings
    init_parquet(str(parquet_dir), str(market_data_path), str(tickers_path))

    return {
        'parquet_dir': str(parquet_dir),
        'csv_path': str(market_data_path),
        'tickers_path': str(tickers_path)
    }

# Tests

def test_parquet_directory_creation(parquet_dataset):
    """Test that Parquet directory is created."""
    p_dir = parquet_dataset['parquet_dir']
    assert os.path.exists(p_dir)
    assert os.path.isdir(p_dir)

def test_parquet_partitioning(parquet_dataset):
    """Test that data is partitioned by ticker."""
    p_dir = parquet_dataset['parquet_dir']
    
    # Check that partition directories exist
    aapl_partition = os.path.join(p_dir, 'ticker=AAPL')
    tsla_partition = os.path.join(p_dir, 'ticker=TSLA')
    
    assert os.path.exists(aapl_partition)
    assert os.path.exists(tsla_partition)
    
    # Check that partition directories contain parquet files
    aapl_files = [f for f in os.listdir(aapl_partition) if f.endswith('.parquet')]
    tsla_files = [f for f in os.listdir(tsla_partition) if f.endswith('.parquet')]
    
    assert len(aapl_files) > 0
    assert len(tsla_files) > 0

def test_load_parquet(parquet_dataset):
    """Test loading Parquet data into DataFrame."""
    p_dir = parquet_dataset['parquet_dir']
    df = load_parquet(p_dir)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert 'ticker' in df.columns
    assert 'timestamp' in df.columns
    assert 'open' in df.columns
    assert 'close' in df.columns
    
    # Check that timestamp is datetime
    assert pd.api.types.is_datetime64_any_dtype(df['timestamp'])

def test_get_ticker_data_parquet(parquet_dataset):
    """Test retrieving ticker data for date range."""
    p_dir = parquet_dataset['parquet_dir']
    df = get_ticker_data_parquet(p_dir, 'AAPL', '2025-11-17', '2025-11-18')
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert all(df['ticker'] == 'AAPL')
    
    # Check date filtering
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    assert all(df['timestamp'] >= pd.to_datetime('2025-11-17'))
    assert all(df['timestamp'] <= pd.to_datetime('2025-11-18'))

def test_get_avg_daily_volume_parquet(parquet_dataset):
    """Test calculating average daily volume per ticker."""
    p_dir = parquet_dataset['parquet_dir']
    df = get_avg_daily_volume_parquet(p_dir)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert 'symbol' in df.columns
    assert 'avg_daily_volume' in df.columns
    
    # Check that all tickers are present
    symbols = set(df['symbol'].unique())
    assert 'AAPL' in symbols
    assert 'TSLA' in symbols
    
    # Check that volumes are positive integers
    assert all(df['avg_daily_volume'] > 0)
    assert all(df['avg_daily_volume'] == df['avg_daily_volume'].astype(int))

def test_get_top_tickers_by_return_parquet(parquet_dataset):
    """Test identifying top tickers by return."""
    p_dir = parquet_dataset['parquet_dir']
    df = get_top_tickers_by_return_parquet(p_dir, '2025-11-17', '2025-11-18', top_n=2)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) <= 2
    assert 'symbol' in df.columns
    assert 'return_pct' in df.columns
    
    # Check that returns are sorted descending
    if len(df) > 1:
        returns = df['return_pct'].values
        # assert that i is >= i+1 for all items
        assert all(returns[i] >= returns[i+1] for i in range(len(returns)-1))

def test_get_daily_trade_summary_parquet(parquet_dataset):
    """Test getting first and last trade prices per day."""
    p_dir = parquet_dataset['parquet_dir']
    df = get_daily_trade_summary_parquet(p_dir)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert 'symbol' in df.columns
    assert 'timestamp' in df.columns
    assert 'first_trade_price' in df.columns
    assert 'last_trade_price' in df.columns
    
    # Check that prices are valid
    assert all(df['first_trade_price'] > 0)
    assert all(df['last_trade_price'] > 0)

def test_get_daily_trade_summary_parquet_with_limit(parquet_dataset):
    """Test daily trade summary with limit."""
    p_dir = parquet_dataset['parquet_dir']
    df = get_daily_trade_summary_parquet(p_dir, limit=3)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) <= 3

def test_task1_aapl_rolling_average(parquet_dataset):
    """Test computing rolling average for AAPL."""
    p_dir = parquet_dataset['parquet_dir']
    df = task1_aapl_rolling_average(p_dir, window=5)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert all(df['ticker'] == 'AAPL')
    assert 'close' in df.columns
    assert 'close_5min_ma' in df.columns
    
    # Check that rolling average is computed
    assert not df['close_5min_ma'].isnull().all()

def test_task2_rolling_volatility(parquet_dataset):
    """Test computing rolling volatility."""
    p_dir = parquet_dataset['parquet_dir']
    df = task2_rolling_volatility(p_dir, window=5)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert 'ticker' in df.columns
    assert 'returns' in df.columns
    assert 'volatility_5d' in df.columns
    
    # Check that volatility is computed (may have NaN for first few rows)
    assert not df['volatility_5d'].isnull().all()

def test_get_parquet_size(parquet_dataset):
    """Test calculating Parquet directory size."""
    p_dir = parquet_dataset['parquet_dir']
    size = get_parquet_size(p_dir)
    
    assert size > 0
    assert isinstance(size, (int, float))

def test_partition_pruning(parquet_dataset):
    """Test that partition pruning works correctly."""
    p_dir = parquet_dataset['parquet_dir']
    
    # Query for AAPL should only read AAPL partition
    aapl_df = get_ticker_data_parquet(p_dir, 'AAPL', '2025-11-17', '2025-11-18')
    
    # Query for TSLA should only read TSLA partition
    tsla_df = get_ticker_data_parquet(p_dir, 'TSLA', '2025-11-17', '2025-11-18')
    
    # Both should work and return correct data
    assert len(aapl_df) > 0
    assert len(tsla_df) > 0
    assert all(aapl_df['ticker'] == 'AAPL')
    assert all(tsla_df['ticker'] == 'TSLA')

def test_data_integrity(parquet_dataset):
    """Test that data integrity is maintained in Parquet."""
    p_dir = parquet_dataset['parquet_dir']
    csv_path = parquet_dataset['csv_path']
    
    original_df = pd.read_csv(csv_path)
    parquet_df = load_parquet(p_dir)
    
    # Check that all rows are preserved
    assert len(original_df) == len(parquet_df)
    
    # Check that all tickers are present
    original_tickers = set(original_df['ticker'].unique())
    parquet_tickers = set(parquet_df['ticker'].unique())
    assert original_tickers == parquet_tickers