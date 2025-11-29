# Market Data Storage and Querying with SQLite3 and Parquet

A Python-based system for ingesting, storing, and querying multi-ticker market data using both SQLite3 and Parquet formats. This project explores the tradeoffs between relational and columnar storage formats, demonstrating how each can be used to support financial analytics and trading workflows.

---

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Dependencies](#dependencies)
- [Project Structure](#project-structure)
- [Module Descriptions](#module-descriptions)
- [Usage Examples](#usage-examples)
- [Running Tests](#running-tests)
- [Query Tasks](#query-tasks)
- [Performance Comparison](#performance-comparison)

---

## Overview

This project implements a complete data engineering pipeline for financial market data:

1. **Data Ingestion:** Load and validate multi-ticker OHLCV data from CSV
2. **SQLite3 Storage:** Store data in a normalized relational database
3. **Parquet Storage:** Store data in a partitioned columnar format
4. **Querying:** Execute time-series and cross-sectional analysis queries
5. **Comparison:** Evaluate performance and storage tradeoffs

### Learning Objectives

- Ingest and validate multi-ticker OHLCV data using Python
- Store structured financial data in both SQLite3 and Parquet formats
- Design and execute SQL queries for time-series and cross-sectional analysis
- Compare performance and storage tradeoffs between relational and columnar formats
- Understand the role of databases in trading systems and financial research

---

## Installation

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

### Setup Steps

1. **Clone or navigate to the project directory:**
   ```bash
   cd market_data_storage_querying_with_sqlite3_parquet
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify installation:**
   ```bash
   python3 -c "import pandas, pyarrow; print('Dependencies installed successfully')"
   ```

---

## Dependencies

The project requires the following Python packages (see `requirements.txt`):

- **pandas** (>=2.0.0): Data manipulation and analysis
- **pyarrow** (>=10.0.0): Parquet file format support
- **sqlite3**: Built-in Python module (no installation needed)

Install all dependencies with:
```bash
pip install -r requirements.txt
```

---

## Project Structure

```
market_data_storage_querying_with_sqlite3_parquet/
├── data_loader.py          # Data loading and validation
├── sqlite_storage.py       # SQLite3 database operations
├── parquet_storage.py      # Parquet file operations
├── schema.sql              # Database schema definition
├── market_data_multi.csv   # Source market data (CSV)
├── tickers.csv             # Ticker reference file
├── market_data.db          # SQLite3 database file
├── market_data/            # Partitioned Parquet directory
│   ├── ticker=AAPL/
│   ├── ticker=AMZN/
│   ├── ticker=GOOG/
│   ├── ticker=MSFT/
│   └── ticker=TSLA/
├── query_tasks.md          # Query task descriptions and results
├── comparison.md           # Format comparison and use case analysis
├── requirements.txt        # Python dependencies
├── README.md              # This file
└── tests/                 # Unit tests (to be created)
```

---

## Module Descriptions

### `data_loader.py`

**Purpose:** Load and validate multi-ticker market data from CSV files.

**Key Functions:**
- `load_and_validate_data(market_data_path, tickers_ref_path)`: Loads CSV data and performs validation

**Features:**
- Validates timestamps and price data
- Checks for missing values
- Verifies all tickers from reference file are present
- Normalizes column names and datetime formatting

**Usage:**
```python
from data_loader import load_and_validate_data

df = load_and_validate_data('market_data_multi.csv', 'tickers.csv')
```

---

### `sqlite_storage.py`

**Purpose:** Create SQLite3 database, insert data, and execute SQL queries.

**Key Functions:**
- `init_db(connection)`: Initialize database schema
- `import_tickers(connection, csv_path)`: Import ticker reference data
- `import_prices(connection, prices_path, tickers_path)`: Import price data
- `get_ticker_data(conn, ticker, start_date, end_date)`: Retrieve ticker data for date range
- `get_avg_daily_volume(conn)`: Calculate average daily volume per ticker
- `get_top_tickers_by_return(conn, start_date, end_date, top_n=3)`: Find top N tickers by return
- `get_daily_trade_summary(conn, limit=None)`: Get first/last trade prices per ticker per day

**Database Schema:**
- `tickers` table: ticker_id, symbol, name, exchange
- `prices` table: id, timestamp, ticker_id, open, high, low, close, volume

**Usage:**
```python
import sqlite3
from sqlite_storage import *

conn = sqlite3.connect('market_data.db')
init_db(conn)
import_tickers(conn, 'tickers.csv')
import_prices(conn, 'market_data_multi.csv', 'tickers.csv')

# Run queries
df = get_ticker_data(conn, 'TSLA', '2025-11-17', '2025-11-18')
```

---

### `parquet_storage.py`

**Purpose:** Convert data to Parquet format and perform columnar queries.

**Key Functions:**
- `init_parquet(parquet_dir, csv_path, tickers_path)`: Create partitioned Parquet dataset
- `load_parquet(parquet_path)`: Load Parquet data into DataFrame
- `get_ticker_data_parquet(parquet_path, ticker, start_date, end_date)`: Retrieve ticker data (optimized for partitions)
- `get_avg_daily_volume_parquet(parquet_path)`: Calculate average daily volume
- `get_top_tickers_by_return_parquet(parquet_path, start_date, end_date, top_n=3)`: Find top tickers by return
- `get_daily_trade_summary_parquet(parquet_path, limit=None)`: Get daily trade summaries
- `task1_aapl_rolling_average(parquet_path, window=5)`: Compute 5-minute rolling average for AAPL
- `task2_rolling_volatility(parquet_path, window=5)`: Compute 5-day rolling volatility
- `task3_benchmark_comparison(parquet_path, ...)`: Compare performance with SQLite3

**Features:**
- Partitioned by ticker symbol for efficient queries
- Optimized partition pruning for single-ticker queries
- Columnar storage for analytical workloads

**Usage:**
```python
from parquet_storage import *

# Initialize partitioned dataset
init_parquet('market_data', 'market_data_multi.csv', 'tickers.csv')

# Run queries
df = get_ticker_data_parquet('market_data', 'TSLA', '2025-11-17', '2025-11-18')
aapl_ma = task1_aapl_rolling_average('market_data')
volatility = task2_rolling_volatility('market_data')
benchmark = task3_benchmark_comparison('market_data')
```

---

## Usage Examples

### Example 1: Initialize SQLite Database

```python
import sqlite3
from sqlite_storage import init_db, import_tickers, import_prices

conn = sqlite3.connect('market_data.db')
init_db(conn)
import_tickers(conn, 'tickers.csv')
import_prices(conn, 'market_data_multi.csv', 'tickers.csv')
conn.close()
```

### Example 2: Query SQLite Database

```python
import sqlite3
from sqlite_storage import get_ticker_data, get_top_tickers_by_return

conn = sqlite3.connect('market_data.db')

# Get TSLA data for a date range
tsla_data = get_ticker_data(conn, 'TSLA', '2025-11-17', '2025-11-18')
print(f"Retrieved {len(tsla_data)} records for TSLA")

# Get top 3 tickers by return
top_tickers = get_top_tickers_by_return(conn, '2025-11-17', '2025-11-18', top_n=3)
print(top_tickers)

conn.close()
```

### Example 3: Initialize Parquet Dataset

```python
from parquet_storage import init_parquet

# Create partitioned Parquet dataset
init_parquet('market_data', 'market_data_multi.csv', 'tickers.csv')
```

### Example 4: Query Parquet Dataset

```python
from parquet_storage import (
    get_ticker_data_parquet,
    task1_aapl_rolling_average,
    task2_rolling_volatility,
    task3_benchmark_comparison
)

# Get ticker data (reads only relevant partition)
tsla_data = get_ticker_data_parquet('market_data', 'TSLA', '2025-11-17', '2025-11-18')

# Compute rolling average for AAPL
aapl_ma = task1_aapl_rolling_average('market_data', window=5)

# Compute rolling volatility
volatility = task2_rolling_volatility('market_data', window=5)

# Benchmark comparison
benchmark = task3_benchmark_comparison('market_data')
print(f"Parquet is {benchmark['comparison']['speedup']}")
```

### Example 5: Run Complete Workflow

```python
# Run SQLite workflow
python3 sqlite_storage.py

# Run Parquet workflow
python3 parquet_storage.py
```

---

## Running Tests

Unit tests are located in the `tests/` directory (to be created).

### Running All Tests

```bash
# Using pytest (recommended)
pytest tests/

# Or using unittest
python3 -m unittest discover tests/
```

### Running Specific Test Files

```bash
# Test data loading
pytest tests/test_data_loader.py

# Test SQLite operations
pytest tests/test_sqlite_storage.py

# Test Parquet operations
pytest tests/test_parquet_storage.py
```

---

## Query Tasks

See `query_tasks.md` for detailed query task descriptions and results.

### SQLite3 Tasks

1. Retrieve all data for TSLA between 2025-11-17 and 2025-11-18
2. Calculate average daily volume per ticker
3. Identify top 3 tickers by return over the full period
4. Find first and last trade price for each ticker per day

### Parquet Tasks

1. Load all data for AAPL and compute 5-minute rolling average of close price
2. Compute 5-day rolling volatility (std dev) of returns for each ticker
3. Compare query time and file size with SQLite3 for Task 1

---

## ⚡ Performance Comparison

See `comparison.md` for detailed performance analysis and use case recommendations.

### Quick Comparison

**File Sizes:**
- SQLite3: ~1.3 MB
- Parquet (partitioned): ~387 KB

**Query Performance:**
- SQLite3: Optimized for transactional queries
- Parquet: Optimized for analytical queries with partition pruning

**Use Cases:**
- **SQLite3:** Best for transactional workloads, complex joins, ACID compliance
- **Parquet:** Best for analytical workloads, columnar operations, data lake storage

---

## File Descriptions

| File | Description |
|------|-------------|
| `data_loader.py` | Data loading and validation module |
| `sqlite_storage.py` | SQLite3 database operations and queries |
| `parquet_storage.py` | Parquet file operations and queries |
| `schema.sql` | SQLite3 database schema definition |
| `market_data_multi.csv` | Source market data (OHLCV format) |
| `tickers.csv` | Ticker reference file |
| `market_data.db` | SQLite3 database file |
| `market_data/` | Partitioned Parquet directory |
| `query_tasks.md` | Query task descriptions and results |
| `comparison.md` | Format comparison and use case analysis |
| `requirements.txt` | Python dependencies |

---

## Key Features

### Data Validation
- Validates timestamps and price data
- Checks for missing values
- Verifies ticker presence
- Normalizes data formats

### SQLite3 Features
- Normalized relational schema
- Efficient indexing
- SQL query support
- ACID compliance

### Parquet Features
- Partitioned by ticker symbol
- Columnar storage format
- Partition pruning optimization
- Efficient analytical queries
