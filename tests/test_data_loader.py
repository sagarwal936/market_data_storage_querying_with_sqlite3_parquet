import pytest
import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_loader import load_and_validate_data

@pytest.fixture
def sample_data(tmp_path):
    """
    Fixture that creates sample CSV files in a temporary directory.
    Returns the paths to market_data and tickers files.
    """
    market_data_path = tmp_path / 'market_data_multi.csv'
    tickers_path = tmp_path / 'tickers.csv'

    # Create sample market data CSV
    sample_data = {
        'timestamp': [
            '2025-11-17 09:30:00', '2025-11-17 09:31:00', '2025-11-17 09:32:00',
            '2025-11-17 09:30:00', '2025-11-17 09:31:00'
        ],
        'ticker': ['AAPL', 'AAPL', 'AAPL', 'TSLA', 'TSLA'],
        'open': [271.45, 269.12, 270.36, 250.00, 251.00],
        'high': [272.07, 269.38, 271.24, 251.00, 252.00],
        'low': [270.77, 269.0, 270.22, 249.00, 250.00],
        'close': [270.88, 269.24, 270.86, 250.50, 251.50],
        'volume': [1416, 3812, 3046, 2000, 2100]
    }
    pd.DataFrame(sample_data).to_csv(market_data_path, index=False)

    # Create sample tickers CSV
    tickers_data = {
        'ticker_id': [1, 2],
        'symbol': ['AAPL', 'TSLA'],
        'name': ['Apple Inc.', 'Tesla Inc.'],
        'exchange': ['NASDAQ', 'NASDAQ']
    }
    pd.DataFrame(tickers_data).to_csv(tickers_path, index=False)

    return market_data_path, tickers_path

def test_load_valid_data(sample_data):
    """Test loading valid market data."""
    market_path, tickers_path = sample_data
    df = load_and_validate_data(market_path, tickers_path)

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert 'ticker' in df.columns
    assert 'timestamp' in df.columns
    assert 'open' in df.columns
    assert 'close' in df.columns

def test_validate_tickers_present(sample_data):
    """Test that all tickers from reference file are present."""
    market_path, tickers_path = sample_data
    df = load_and_validate_data(market_path, tickers_path)

    tickers_ref = pd.read_csv(tickers_path)
    expected_tickers = set(tickers_ref['symbol'].unique())
    actual_tickers = set(df['ticker'].unique())

    assert expected_tickers.issubset(actual_tickers)

def test_missing_ticker_raises_error(sample_data, tmp_path):
    """Test that missing tickers raise ValueError."""
    market_path, _ = sample_data
    
    # Create tickers CSV with a ticker not in market data
    tickers_data = {
        'ticker_id': [1, 2, 3],
        'symbol': ['AAPL', 'TSLA', 'MSFT'],  # MSFT not in market data
        'name': ['Apple Inc.', 'Tesla Inc.', 'Microsoft Corp.'],
        'exchange': ['NASDAQ', 'NASDAQ', 'NASDAQ']
    }
    tickers_path = tmp_path / 'tickers_missing.csv'
    pd.DataFrame(tickers_data).to_csv(tickers_path, index=False)

    with pytest.raises(ValueError, match="missing"):
        load_and_validate_data(market_path, tickers_path)

def test_missing_timestamps_raises_error(sample_data, tmp_path):
    """Test that missing timestamps raise ValueError."""
    _, tickers_path = sample_data
    
    sample_data_missing = {
        'timestamp': ['2025-11-17 09:30:00', None, '2025-11-17 09:32:00'],
        'ticker': ['AAPL', 'AAPL', 'AAPL'],
        'open': [271.45, 269.12, 270.36],
        'high': [272.07, 269.38, 271.24],
        'low': [270.77, 269.0, 270.22],
        'close': [270.88, 269.24, 270.86],
        'volume': [1416, 3812, 3046]
    }
    invalid_path = tmp_path / 'invalid_data.csv'
    pd.DataFrame(sample_data_missing).to_csv(invalid_path, index=False)

    with pytest.raises(ValueError, match="timestamp"):
        load_and_validate_data(invalid_path, tickers_path)

def test_missing_prices_raises_error(sample_data, tmp_path):
    """Test that missing price values raise ValueError."""
    _, tickers_path = sample_data
    
    sample_data_missing_price = {
        'timestamp': ['2025-11-17 09:30:00', '2025-11-17 09:31:00', '2025-11-17 09:32:00'],
        'ticker': ['AAPL', 'AAPL', 'AAPL'],
        'open': [271.45, None, 270.36],  # Missing price
        'high': [272.07, 269.38, 271.24],
        'low': [270.77, 269.0, 270.22],
        'close': [270.88, None, 270.86],  # Missing price
        'volume': [1416, 3812, 3046]
    }
    invalid_path = tmp_path / 'invalid_prices.csv'
    pd.DataFrame(sample_data_missing_price).to_csv(invalid_path, index=False)

    with pytest.raises(ValueError, match="price"):
        load_and_validate_data(invalid_path, tickers_path)

def test_normalize_column_names(sample_data, tmp_path):
    """Test that column names are normalized to lowercase."""
    _, tickers_path = sample_data
    
    # FIX: Added TSLA to match the tickers.csv file which expects both AAPL and TSLA
    sample_data_uppercase = {
        'Timestamp': ['2025-11-17 09:30:00', '2025-11-17 09:31:00'],
        'Ticker': ['AAPL', 'TSLA'],  # changed second AAPL to TSLA
        'Open': [271.45, 250.00],
        'High': [272.07, 251.00],
        'Low': [270.77, 249.00],
        'Close': [270.88, 250.50],
        'Volume': [1416, 2000]
    }
    uppercase_path = tmp_path / 'uppercase_data.csv'
    pd.DataFrame(sample_data_uppercase).to_csv(uppercase_path, index=False)

    result_df = load_and_validate_data(uppercase_path, tickers_path)

    assert 'timestamp' in result_df.columns
    assert 'ticker' in result_df.columns
    assert 'open' in result_df.columns
    assert 'Timestamp' not in result_df.columns
    assert 'Ticker' not in result_df.columns

def test_datetime_conversion(sample_data):
    """Test that timestamps are converted to datetime."""
    market_path, tickers_path = sample_data
    df = load_and_validate_data(market_path, tickers_path)

    assert pd.api.types.is_datetime64_any_dtype(df['timestamp'])

def test_file_not_found_error(sample_data):
    """Test that missing files raise FileNotFoundError."""
    market_path, tickers_path = sample_data
    
    with pytest.raises(FileNotFoundError):
        load_and_validate_data('nonexistent.csv', tickers_path)
    
    with pytest.raises(FileNotFoundError):
        load_and_validate_data(market_path, 'nonexistent.csv')