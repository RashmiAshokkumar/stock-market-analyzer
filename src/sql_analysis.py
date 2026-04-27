# src/sql_analysis.py

from sqlalchemy import create_engine, text
import pandas as pd
import os

# ── Database connection ────────────────────────────────────────────────────────
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH  = os.path.join(BASE_DIR, "database", "stocks.db")
engine   = create_engine(f"sqlite:///{DB_PATH}")

def run_query(sql: str) -> pd.DataFrame:
    """Helper — runs any SQL string and returns a DataFrame."""
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    # Clean up date column — strip the time portion for display
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


# ── Query 1: 20-day and 50-day moving average ─────────────────────────────────

# Moving averages smooth out daily noise so you can see the real trend.
# Traders use 20-day (short term) and 50-day (long term) constantly.
# When 20MA crosses above 50MA → bullish signal. Below → bearish.

def moving_averages(ticker: str) -> pd.DataFrame:
    """
    Calculates 20-day and 50-day moving averages for a stock.
    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW = look back 20 rows including today.
    """
    sql = f"""
        SELECT
            date,
            ticker,
            close,
            ROUND(AVG(close) OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ), 2) AS ma_20,
            ROUND(AVG(close) OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            ), 2) AS ma_50
        FROM stock_prices
        WHERE ticker = '{ticker}'
        ORDER BY date
    """
    return run_query(sql)


# ── Query 2: Daily return % ───────────────────────────────────────────────────

# Daily return = how much the stock gained or lost each day in percentage.
# Formula: (today_close - yesterday_close) / yesterday_close * 100
# LAG(close, 1) looks at the previous row's close price.

def daily_returns(ticker: str) -> pd.DataFrame:
    """
    Calculates day-over-day percentage return using LAG window function.
    LAG(close, 1) = the close price from 1 row before (yesterday).
    """
    sql = f"""
        SELECT
            date,
            ticker,
            close,
            ROUND(
                (close - LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date))
                / LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date)
                * 100,
            2) AS daily_return_pct
        FROM stock_prices
        WHERE ticker = '{ticker}'
        ORDER BY date
    """
    return run_query(sql)


# ── Query 3: Monthly performance comparison ───────────────────────────────────

# Which stock performed best each month?
# We take the first close of the month and last close of the month,
# then calculate the return between them.

def monthly_performance() -> pd.DataFrame:
    """
    Compares all stocks month by month.
    Uses a CTE to get first and last close per month per ticker,
    then calculates the return between them.
    """
    sql = """
        WITH monthly_bounds AS (
            SELECT
                ticker,
                STRFTIME('%Y-%m', date) AS month,
                MIN(date) AS first_date,
                MAX(date) AS last_date
            FROM stock_prices
            GROUP BY ticker, STRFTIME('%Y-%m', date)
        ),
        monthly_prices AS (
            SELECT
                mb.ticker,
                mb.month,
                sp_open.close AS month_open,
                sp_close.close AS month_close
            FROM monthly_bounds mb
            JOIN stock_prices sp_open
                ON mb.ticker = sp_open.ticker AND mb.first_date = sp_open.date
            JOIN stock_prices sp_close
                ON mb.ticker = sp_close.ticker AND mb.last_date = sp_close.date
        )
        SELECT
            ticker,
            month,
            ROUND(month_open, 2)  AS month_open,
            ROUND(month_close, 2) AS month_close,
            ROUND((month_close - month_open) / month_open * 100, 2) AS monthly_return_pct
        FROM monthly_prices
        ORDER BY month DESC, monthly_return_pct DESC
    """
    return run_query(sql)


# ── Query 4: Rolling 30-day volatility ───────────────────────────────────────

# Volatility = standard deviation of daily returns over 30 days.
# High volatility = risky stock (price swings a lot). Low = stable.
# TSLA should show much higher volatility than JPM.

def rolling_volatility(ticker: str) -> pd.DataFrame:
    """
    Calculates 30-day rolling volatility (std dev of daily returns).
    This is a core risk metric used by every finance professional.
    """
    sql = f"""
        WITH daily AS (
            SELECT
                date,
                ticker,
                close,
                (close - LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date))
                / LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date)
                * 100 AS daily_return
            FROM stock_prices
            WHERE ticker = '{ticker}'
        )
        SELECT
            date,
            ticker,
            close,
            ROUND(daily_return, 4) AS daily_return,
            ROUND(AVG(daily_return) OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ), 4) AS rolling_avg_30d
        FROM daily
        ORDER BY date
    """
    return run_query(sql)


# ── Query 5: Rank stocks by total 2-year return ───────────────────────────────
# Which stock grew the most over the full 2 years?
# RANK() assigns position 1 to the best performer.

def rank_by_total_return() -> pd.DataFrame:
    """
    Ranks all stocks by their total return over the full date range.
    Uses a CTE to find start and end prices, then RANK() on top.
    """
    sql = """
        WITH bounds AS (
            SELECT
                ticker,
                MIN(date) AS first_date,
                MAX(date) AS last_date
            FROM stock_prices
            GROUP BY ticker
        ),
        prices AS (
            SELECT
                b.ticker,
                sp_start.close AS start_price,
                sp_end.close   AS end_price
            FROM bounds b
            JOIN stock_prices sp_start
                ON b.ticker = sp_start.ticker AND b.first_date = sp_start.date
            JOIN stock_prices sp_end
                ON b.ticker = sp_end.ticker AND b.last_date = sp_end.date
        )
        SELECT
            ticker,
            ROUND(start_price, 2) AS start_price,
            ROUND(end_price,   2) AS end_price,
            ROUND((end_price - start_price) / start_price * 100, 2) AS total_return_pct,
            RANK() OVER (ORDER BY (end_price - start_price) / start_price DESC) AS rank
        FROM prices
        ORDER BY rank
    """
    return run_query(sql)


# ── Run all queries ───────────────────────────────────────────────────────────
if __name__ == "__main__":

    print("=" * 60)
    print("QUERY 1: 20 & 50 Day Moving Averages — AAPL (last 10 rows)")
    print("=" * 60)
    df1 = moving_averages("AAPL")
    print(df1.tail(10).to_string(index=False))

    print("\n" + "=" * 60)
    print("QUERY 2: Daily Returns — AAPL (last 10 rows)")
    print("=" * 60)
    df2 = daily_returns("AAPL")
    print(df2.tail(10).to_string(index=False))

    print("\n" + "=" * 60)
    print("QUERY 3: Monthly Performance — All Stocks (last 3 months)")
    print("=" * 60)
    df3 = monthly_performance()
    print(df3.head(15).to_string(index=False))

    print("\n" + "=" * 60)
    print("QUERY 4: Rolling 30-day Volatility — TSLA vs AAPL")
    print("=" * 60)
    df4_tsla = rolling_volatility("TSLA")
    df4_aapl = rolling_volatility("AAPL")
    print("TSLA (last 5):")
    print(df4_tsla.tail(5).to_string(index=False))
    print("\nAAPL (last 5):")
    print(df4_aapl.tail(5).to_string(index=False))

    print("\n" + "=" * 60)
    print("QUERY 5: Stock Rankings by Total 2-Year Return")
    print("=" * 60)
    df5 = rank_by_total_return()
    print(df5.to_string(index=False))