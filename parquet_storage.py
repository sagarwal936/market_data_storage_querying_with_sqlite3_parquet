import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlite3
import os
import time
from data_loader import load_and_validate_data
from sqlite_storage import get_ticker_data, DB_NAME

PARQUET_DIR = 'market_data'
TICKERS_FILE = 'tickers.csv'
PRICES_FILE = 'market_data_multi.csv'

def init_parquet(parquet_dir, csv_path, tickers_path):
    """
    Initialize partitioned Parquet dataset from CSV data.
    Creates partitioned directory structure by ticker symbol.
    """
    if os.path.exists(parquet_dir) and os.path.isdir(parquet_dir):
        print(f"Parquet directory {parquet_dir} already exists.")
        return
    
    try:
        clean_df = load_and_validate_data(csv_path, tickers_path)
    except FileNotFoundError as e:
        print(f"Error loading data files: {e}")
        return
    
    if 'timestamp' in clean_df.columns:
        clean_df['timestamp'] = pd.to_datetime(clean_df['timestamp'])
    
    clean_df = clean_df.sort_values(['ticker', 'timestamp'])
    
    table = pa.Table.from_pandas(clean_df)
    
    pq.write_to_dataset(
        table,
        root_path=parquet_dir,
        partition_cols=['ticker']
    )
    print(f"Successfully created partitioned Parquet dataset: {parquet_dir}/")
    print(f"Loaded {len(clean_df)} price records into Parquet.")
    print(f"Partitioned by ticker: {sorted(clean_df['ticker'].unique())}")

def load_parquet(parquet_path):
    """
    Load Parquet data from partitioned directory into pandas DataFrame.
    """
    if not os.path.isdir(parquet_path):
        raise FileNotFoundError(f"Parquet directory not found: {parquet_path}")
    dataset = pq.ParquetDataset(parquet_path)
    table = dataset.read()
    df = table.to_pandas()
    
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    return df

def get_parquet_size(parquet_path):
    """
    Calculate total size of partitioned Parquet directory.
    """
    if not os.path.isdir(parquet_path):
        return 0
    
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(parquet_path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            total_size += os.path.getsize(filepath)
    return total_size


######################################### Data Querying Functions #########################################

def get_ticker_data_parquet(parquet_path, ticker, start_date, end_date):
    """
    Retrieve all data for a ticker between start_date and end_date.
    Optimized to read only the relevant partition.
    """
    ticker_partition = os.path.join(parquet_path, f'ticker={ticker}')
    if os.path.exists(ticker_partition) and os.path.isdir(ticker_partition):
        dataset = pq.ParquetDataset(ticker_partition)
        table = dataset.read()
        df = table.to_pandas()
        if 'ticker' not in df.columns:
            df['ticker'] = ticker
    else:
        df = load_parquet(parquet_path)
    
    mask = (
        (df['timestamp'] >= start_date) &
        (df['timestamp'] <= end_date)
    )
    result = df[mask].copy()
    result = result.sort_values('timestamp')
    
    if 'timestamp' in result.columns:
        result['timestamp'] = pd.to_datetime(result['timestamp'])
    
    return result[['ticker', 'timestamp', 'open', 'high', 'low', 'close', 'volume']]

def get_avg_daily_volume_parquet(parquet_path):
    """
    Calculate average daily volume per ticker.
    """
    df = load_parquet(parquet_path)
    
    result = df.groupby('ticker', observed=True)['volume'].mean().reset_index()
    result.columns = ['symbol', 'avg_daily_volume']
    result['avg_daily_volume'] = result['avg_daily_volume'].astype(int)
    result = result.sort_values('avg_daily_volume', ascending=False)
    
    return result

def get_top_tickers_by_return_parquet(parquet_path, start_date, end_date, top_n=3):
    """
    Identify top N tickers by return over the period.
    """
    df = load_parquet(parquet_path)
    
    mask = (df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)
    period_df = df[mask].copy()
    
    results = []
    for ticker in period_df['ticker'].unique():
        ticker_data = period_df[period_df['ticker'] == ticker].sort_values('timestamp')
        
        if len(ticker_data) == 0:
            continue
        
        start_price = ticker_data.iloc[0]['open']
        end_price = ticker_data.iloc[-1]['close']
        
        if start_price > 0:
            return_pct = ((end_price - start_price) / start_price) * 100
            results.append({
                'symbol': ticker,
                'return_pct': round(return_pct, 2)
            })
    
    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values('return_pct', ascending=False)
    result_df = result_df.head(top_n)
    
    return result_df

def get_daily_trade_summary_parquet(parquet_path, limit=None):
    """
    Find first and last trade price for each ticker per day.
    """
    df = load_parquet(parquet_path)
    
    df['trade_date'] = df['timestamp'].dt.date
    
    results = []
    for (ticker, trade_date), group in df.groupby(['ticker', 'trade_date'], observed=True):
        group_sorted = group.sort_values('timestamp')
        first_price = group_sorted.iloc[0]['open']
        last_price = group_sorted.iloc[-1]['close']
        
        results.append({
            'symbol': ticker,
            'timestamp': trade_date,
            'first_trade_price': first_price,
            'last_trade_price': last_price
        })
    
    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values(['timestamp', 'symbol'], ascending=[False, True])
    
    if limit:
        result_df = result_df.head(limit)
    
    return result_df

def get_all_market_data_parquet(parquet_path, ticker, start_date, end_date):
    """
    Get all market data queries in one call.
    """
    return {
        "ticker_history": get_ticker_data_parquet(parquet_path, ticker, start_date, end_date),
        "volume_summary": get_avg_daily_volume_parquet(parquet_path),
        "top_returns": get_top_tickers_by_return_parquet(parquet_path, start_date, end_date),
        "daily_summary": get_daily_trade_summary_parquet(parquet_path)
    }


######################################### Parquet-Specific Tasks #########################################

def task1_aapl_rolling_average(parquet_path, window=5):
    """
    Load all data for AAPL and compute 5-minute rolling average of close price.
    Optimized to read only AAPL partition.
    """
    aapl_partition = os.path.join(parquet_path, 'ticker=AAPL')
    if os.path.exists(aapl_partition) and os.path.isdir(aapl_partition):
        dataset = pq.ParquetDataset(aapl_partition)
        table = dataset.read()
        df = table.to_pandas()
        if 'ticker' not in df.columns:
            df['ticker'] = 'AAPL'
    else:
        df = load_parquet(parquet_path)
        df = df[df['ticker'] == 'AAPL'].copy()
    
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    df['close_5min_ma'] = df['close'].rolling(window=window, min_periods=1).mean()
    
    return df[['ticker', 'timestamp', 'close', 'close_5min_ma']]

def task2_rolling_volatility(parquet_path, window=5):
    """
    Compute 5-day rolling volatility (std dev) of returns for each ticker.
    """
    df = load_parquet(parquet_path)
    df = df.sort_values(['ticker', 'timestamp'])
    
    df['returns'] = df.groupby('ticker', observed=True)['close'].pct_change()
    df['volatility_5d'] = df.groupby('ticker', observed=True)['returns'].rolling(window=window, min_periods=1).std().reset_index(0, drop=True)
    
    return df[['ticker', 'timestamp', 'close', 'returns', 'volatility_5d']]

def task3_benchmark_comparison(parquet_path, ticker='TSLA', start_date='2025-11-17', end_date='2025-11-18'):
    """
    Compare query time and file size with SQLite3 for Task 1.
    """
    results = {}
    
    # SQLite3 query
    start_time = time.time()
    conn = sqlite3.connect(DB_NAME)
    sqlite_df = get_ticker_data(conn, ticker, start_date, end_date)
    sqlite_time = time.time() - start_time
    conn.close()
    results['sqlite'] = {
        'query_time': sqlite_time,
        'records': len(sqlite_df),
        'file_size': os.path.getsize(DB_NAME) if os.path.exists(DB_NAME) else 0
    }
    
    # Parquet query
    start_time = time.time()
    parquet_df = get_ticker_data_parquet(parquet_path, ticker, start_date, end_date)
    parquet_time = time.time() - start_time
    results['parquet'] = {
        'query_time': parquet_time,
        'records': len(parquet_df),
        'file_size': get_parquet_size(parquet_path)
    }
    
    # Comparison
    if parquet_time > 0:
        time_ratio = sqlite_time / parquet_time
        speedup = f"{time_ratio:.2f}x faster" if parquet_time < sqlite_time else f"{parquet_time / sqlite_time:.2f}x slower"
    else:
        time_ratio = float('inf')
        speedup = "N/A"
    
    if results['parquet']['file_size'] > 0:
        size_ratio = results['sqlite']['file_size'] / results['parquet']['file_size']
        size_diff = ((results['sqlite']['file_size'] - results['parquet']['file_size']) / results['parquet']['file_size'] * 100)
        size_difference = f"{size_diff:.1f}%"
    else:
        size_ratio = 0
        size_difference = "N/A"
    
    results['comparison'] = {
        'time_ratio': time_ratio,
        'size_ratio': size_ratio,
        'speedup': speedup,
        'size_difference': size_difference
    }
    
    return results

def main():
    parquet_path = PARQUET_DIR
    
    if not os.path.exists(parquet_path):
        print("Initializing partitioned Parquet dataset...")
        init_parquet(parquet_path, PRICES_FILE, TICKERS_FILE)
    
    print("\n" + "="*60)
    print("Parquet Queries")
    print("="*60)
    
    dfs = get_all_market_data_parquet(parquet_path, 'AAPL', '2025-11-17', '2025-11-22')
    for key in dfs:
        print(f"\n--- {key} ---")
        print(dfs[key].head())
    
    # Run Parquet-specific tasks
    print("\n" + "="*60)
    print("Parquet Task 1: AAPL 5-minute rolling avg.")
    print("="*60)
    task1_result = task1_aapl_rolling_average(parquet_path)
    print(task1_result.head(10))
    print(f"\nTotal records: {len(task1_result)}")
    
    print("\n" + "="*60)
    print("Parquet Task 2: 5-day rolling vol")
    print("="*60)
    task2_result = task2_rolling_volatility(parquet_path)
    print(task2_result.head(10))
    print(f"\nTotal records: {len(task2_result)}")
    print(f"Tickers: {task2_result['ticker'].nunique()}")
    
    print("\n" + "="*60)
    print("Parquet Task 3: Benchmark comparison")
    print("="*60)
    benchmark_results = task3_benchmark_comparison(parquet_path)
    
    print("\nSQLite Results:")
    print(f"  Query time: {benchmark_results['sqlite']['query_time']:.4f} seconds")
    print(f"  Records: {benchmark_results['sqlite']['records']}")
    print(f"  File size: {benchmark_results['sqlite']['file_size'] / 1024:.2f} KB")
    
    print("\nParquet Results:")
    print(f"  Query time: {benchmark_results['parquet']['query_time']:.4f} seconds")
    print(f"  Records: {benchmark_results['parquet']['records']}")
    print(f"  File size: {benchmark_results['parquet']['file_size'] / 1024:.2f} KB")
    
    print("\nComparison:")
    print(f"  Time ratio: {benchmark_results['comparison']['time_ratio']:.2f}x")
    print(f"  Parquet is {benchmark_results['comparison']['speedup']}")
    print(f"  Size ratio: {benchmark_results['comparison']['size_ratio']:.2f}x")
    print(f"  Size difference: {benchmark_results['comparison']['size_difference']}")

if __name__ == "__main__":
    main()
