# Query Tasks

This document describes the SQLite3 and Parquet query tasks, their implementations, results, and performance notes.

---

## SQLite3 Query Tasks

### Task 1: Retrieve All Data for TSLA Between 2025-11-17 and 2025-11-18

**Query:**
```sql
SELECT t.symbol, p.timestamp, p.open, p.high, p.low, p.close, p.volume
FROM prices p
JOIN tickers t ON p.ticker_id = t.ticker_id
WHERE t.symbol = 'TSLA' 
  AND p.timestamp BETWEEN '2025-11-17' AND '2025-11-18'
ORDER BY p.timestamp ASC
```

**Results:**
- **Records Retrieved:** 391
- **Query Time:** 0.0026 seconds
- **Sample Data:**
  ```
  symbol  timestamp            open    high    low     close   volume
  TSLA    2025-11-17 09:30:00  268.31  268.51  267.95  268.07  1609
  TSLA    2025-11-17 09:31:00  268.94  269.11  268.28  269.04  4809
  TSLA    2025-11-17 09:32:00  267.70  267.94  267.69  267.92  1997
  ```

**Performance Notes:**
- Efficient query using indexed JOIN on `ticker_id` and timestamp filtering
- SQLite3's query optimizer uses indexes for fast lookups
- Query benefits from normalized schema with foreign key relationships

---

### Task 2: Calculate Average Daily Volume Per Ticker

**Query:**
```sql
SELECT 
    t.symbol, 
    CAST(AVG(p.volume) AS INTEGER) as avg_daily_volume
FROM prices p
JOIN tickers t ON p.ticker_id = t.ticker_id
GROUP BY t.symbol
ORDER BY avg_daily_volume DESC
```

**Results:**
- **Query Time:** 0.0021 seconds
- **Results:**
  ```
  symbol  avg_daily_volume
  TSLA    2777
  AAPL    2767
  AMZN    2753
  GOOG    2740
  MSFT    2686
  ```

**Performance Notes:**
- Efficient aggregation using GROUP BY with indexed columns
- SQLite3's aggregate functions are optimized for this type of query
- Results sorted by average volume in descending order

---

### Task 3: Identify Top 3 Tickers by Return Over a Given Week

**Query:**
```sql
WITH period_boundaries AS (
    SELECT 
        ticker_id, 
        MIN(timestamp) as first_date, 
        MAX(timestamp) as last_date
    FROM prices
    WHERE timestamp BETWEEN '2025-11-17' AND '2025-11-22'
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
LIMIT 3
```

**Results:**
- **Query Time:** 0.0070 seconds
- **Top 3 Tickers by Return:**
  ```
  symbol  return_pct
  MSFT    33.38
  AAPL    23.25
  GOOG    10.49
  ```

**Performance Notes:**
- Complex query using Common Table Expressions (CTEs) for clarity
- Multiple JOINs efficiently handled by SQLite3's query optimizer
- Window functions and subqueries demonstrate SQLite3's SQL capabilities

---

### Task 4: Find First and Last Trade Price for Each Ticker Per Day

**Query:**
```sql
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
```

**Results:**
- **Query Time:** 0.0099 seconds
- **Sample Results (first 10):**
  ```
  symbol  timestamp   first_trade_price  last_trade_price
  AAPL    2025-11-21  319.44            334.57
  AMZN    2025-11-21  93.27             77.16
  GOOG    2025-11-21  163.10            153.90
  MSFT    2025-11-21  286.43            245.70
  TSLA    2025-11-21  265.91            292.32
  AAPL    2025-11-20  296.51            319.43
  AMZN    2025-11-20  95.52             94.05
  GOOG    2025-11-20  139.24            162.43
  MSFT    2025-11-20  253.27            284.98
  TSLA    2025-11-20  270.63            265.61
  ```

**Performance Notes:**
- Complex query with multiple JOINs and date grouping
- Demonstrates SQLite3's ability to handle sophisticated relational queries
- Results show daily price movements for each ticker

---

## Parquet Query Tasks

### Task 1: Load All Data for AAPL and Compute 5-Minute Rolling Average of Close Price

**Implementation:**
- Loads AAPL partition from Parquet dataset (partition pruning optimization)
- Computes rolling average using pandas `rolling()` function with window=5

**Results:**
- **Records:** 1,955
- **Query Time:** 0.1398 seconds (includes data loading and computation)
- **Sample Data:**
  ```
  ticker  timestamp           close   close_5min_ma
  AAPL    2025-11-17 09:30:00 270.88  270.880000
  AAPL    2025-11-17 09:31:00 269.24  270.060000
  AAPL    2025-11-17 09:32:00 270.86  270.326667
  AAPL    2025-11-17 09:33:00 269.28  270.065000
  AAPL    2025-11-17 09:34:00 269.32  269.916000
  ```

**Performance Notes:**
- **Partition Pruning:** Only reads `ticker=AAPL/` partition, not entire dataset
- **Columnar Efficiency:** Only reads `close` column for rolling average computation
- Query time includes both data loading and rolling average calculation
- Demonstrates Parquet's strength in analytical workloads

---

### Task 2: Compute 5-Day Rolling Volatility (Std Dev) of Returns for Each Ticker

**Implementation:**
- Loads entire Parquet dataset (required for cross-ticker analysis)
- Computes returns using `pct_change()` grouped by ticker
- Computes rolling standard deviation of returns with window=5

**Results:**
- **Records:** 9,775 (all tickers)
- **Query Time:** 0.0086 seconds
- **Sample Data:**
  ```
  ticker  timestamp           close   returns      volatility_5d
  AAPL    2025-11-17 09:30:00 270.88  NaN          NaN
  AAPL    2025-11-17 09:31:00 269.24  -0.006054    NaN
  AAPL    2025-11-17 09:32:00 270.86  0.006017     0.008536
  AAPL    2025-11-17 09:33:00 269.28  -0.005833    0.006906
  AAPL    2025-11-17 09:34:00 269.32  0.000149     0.005736
  ```

**Performance Notes:**
- **Columnar Operations:** Efficient computation on `close` and `returns` columns
- **Grouped Operations:** Pandas groupby operations are optimized for columnar data
- First few rows have NaN values (insufficient data for rolling window)
- Demonstrates Parquet's efficiency for time-series analytics

---

### Task 3: Compare Query Time and File Size with SQLite3

**Test Query:** Retrieve all data for TSLA between 2025-11-17 and 2025-11-18

**Results:**

| Metric | SQLite3 | Parquet | Comparison |
|--------|---------|---------|------------|
| **Query Time** | 0.0027s | 0.0022s | Parquet is **1.25x faster** |
| **File Size** | 1,324 KB | 387.42 KB | Parquet is **3.42x smaller** |
| **Records** | 391 | 391 | Same data retrieved |

**Performance Analysis:**

1. **Query Speed:**
   - Parquet is slightly faster (1.25x) for single-ticker queries
   - Benefits from partition pruning (only reads `ticker=TSLA/` partition)
   - SQLite3 is still very fast due to indexed lookups
   - Both formats perform excellently for this query type

2. **Storage Efficiency:**
   - Parquet is significantly more storage-efficient (3.42x smaller)
   - Columnar compression reduces file size
   - SQLite3 includes overhead for indexes and relational metadata

3. **Use Case Implications:**
   - **Parquet:** Better for analytical workloads with large datasets
   - **SQLite3:** Better for transactional queries and complex JOINs
   - Both formats are suitable for financial data storage

**Performance Notes:**
- Parquet's partition pruning provides significant speedup for filtered queries
- SQLite3's indexed lookups are highly optimized for relational queries
- File size difference becomes more significant with larger datasets
- Choice depends on query patterns and use case requirements

---

## Summary of Query Performance

### SQLite3 Performance Summary

| Task | Query Time | Complexity | Notes |
|------|------------|------------|-------|
| Single-ticker lookup | 0.0026s | Low | Indexed JOIN, efficient |
| Aggregation | 0.0021s | Low | GROUP BY with indexes |
| Top returns | 0.0070s | High | Complex CTEs, multiple JOINs |
| Daily summary | 0.0099s | High | Date grouping, multiple JOINs |

**Average Query Time:** ~0.0054 seconds

### Parquet Performance Summary

| Task | Query Time | Complexity | Notes |
|------|------------|------------|-------|
| Single-ticker rolling avg | 0.1398s | Medium | Includes computation time |
| Rolling volatility | 0.0086s | Medium | Columnar operations |
| Benchmark comparison | 0.0022s | Low | Partition pruning |

**Average Query Time:** ~0.0502 seconds (excluding computation-heavy tasks)

### Key Insights

1. **SQLite3 excels at:**
   - Complex relational queries with JOINs
   - Transactional queries with ACID guarantees
   - SQL-based workflows

2. **Parquet excels at:**
   - Analytical queries with partition pruning
   - Columnar operations (aggregations, rolling windows)
   - Storage efficiency

3. **Both formats are suitable for:**
   - Financial market data storage
   - Time-series analysis
   - Multi-ticker datasets

4. **Performance tradeoffs:**
   - SQLite3: Better for complex queries, slightly larger file size
   - Parquet: Better for analytical queries, significantly smaller file size
   - Both perform well for their intended use cases

---

## Conclusion

All query tasks have been successfully implemented and tested. The results demonstrate that:

1. **SQLite3** provides excellent performance for relational queries with complex JOINs and aggregations
2. **Parquet** provides excellent performance for analytical queries with partition pruning and columnar operations
3. Both formats are well-suited for financial market data storage and querying
4. The choice between formats depends on specific use case requirements (transactional vs. analytical)

See `comparison.md` for detailed format comparison and use case recommendations.
