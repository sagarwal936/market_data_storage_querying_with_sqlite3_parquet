import pandas as pd
import os

def load_and_validate_data(market_data_path, tickers_ref_path):
    print(f"--- Loading data from {market_data_path} ---")
    
    try:
        df = pd.read_csv(market_data_path)
        tickers_ref_df = pd.read_csv(tickers_ref_path)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Could not find file: {e.filename}")

    df.columns = df.columns.str.lower().str.strip()
    tickers_ref_df.columns = tickers_ref_df.columns.str.lower().str.strip()
    
    expected_cols = ['timestamp', 'ticker', 'open', 'high', 'low', 'close', 'volume']
    if not all(col in df.columns for col in expected_cols):
        print(f"Warning: Columns found {df.columns}. Expected standard OHLCV headers.")

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    print("--- Validating Data ---")

    if df['timestamp'].isnull().any():
        raise ValueError("Data validation failed: Found invalid or missing dates/timestamps.")
    
    price_cols = ['open', 'high', 'low', 'close', 'volume']
    cols_to_check = [c for c in price_cols if c in df.columns]
    
    if df[cols_to_check].isnull().values.any():
        missing_count = df[cols_to_check].isnull().sum().sum()
        raise ValueError(f"Data validation failed: Found {missing_count} missing price/volume values.")

    ref_ticker_col = 'symbol' if 'symbol' in tickers_ref_df.columns else tickers_ref_df.columns[0]
    expected_tickers = set(tickers_ref_df[ref_ticker_col].unique())
    actual_tickers = set(df['ticker'].unique())
    missing_tickers = expected_tickers - actual_tickers
    if missing_tickers:
        raise ValueError(f"Data validation failed: The following tickers from tickers.csv are missing in the market data: {missing_tickers}")
    else:
        print("Ticker validation passed: All reference tickers are present.")

    print("Successfully loaded and validated data.")
    return df

if __name__ == "__main__":
    clean_df = load_and_validate_data('market_data_multi.csv', 'tickers.csv')
    print(clean_df.head())
    print(clean_df.dtypes)
