# src/data_ingestion.py

import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
import os

# ─── Database connection ───────────────────────────────────────────────────────
# os.path gives us the location of THIS file (src/)
# We go one level up (..) to reach the project root, then into database/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, "database", "stocks.db")

# SQLAlchemy creates a connection "engine" to our SQLite file
# sqlite:/// means a local file path
engine = create_engine(f"sqlite:///{DB_PATH}")


# ─── Fetch function ────────────────────────────────────────────────────────────
def fetch_stock_data(ticker: str, period: str = "1y") -> pd.DataFrame:
    """
    Downloads historical OHLCV data for a given stock ticker.

    Args:
        ticker: Stock symbol e.g. 'AAPL', 'MSFT', 'TSLA'
        period: How far back to go. Options: 1d,5d,1mo,3mo,6mo,1y,2y,5y

    Returns:
        A cleaned pandas DataFrame with OHLCV columns
    """
    print(f"Fetching data for {ticker}...")

    # Download from Yahoo Finance
    raw = yf.Ticker(ticker).history(period=period)

    # If nothing came back, the ticker probably doesn't exist
    if raw.empty:
        raise ValueError(f"No data found for ticker: {ticker}")

    return raw


# ─── Clean function ────────────────────────────────────────────────────────────
def clean_stock_data(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Cleans raw yfinance data — fixes timezone, resets index,
    renames columns, and adds a ticker column.

    Args:
        df:     Raw DataFrame from fetch_stock_data()
        ticker: Stock symbol string (we store this as a column)

    Returns:
        A clean, analysis-ready DataFrame
    """
    # yfinance returns the date as the index — move it to a regular column
    df = df.reset_index()

    # The Date column has timezone info (the -04:00 you saw earlier)
    # .dt.tz_localize(None) strips the timezone so SQL stores it cleanly
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)

    # Keep only the columns we care about
    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]

    # Standardise column names to lowercase — cleaner for SQL queries
    df.columns = ["date", "open", "high", "low", "close", "volume"]

    # Add ticker column so we know which stock each row belongs to
    # This is critical — all stocks go in the same table
    df["ticker"] = ticker.upper()

    # Round prices to 4 decimal places — yfinance sometimes gives 10+
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].round(4)

    # Drop any rows where close price is missing
    df = df.dropna(subset=["close"])

    print(f"  Cleaned {len(df)} rows for {ticker}")
    return df


# ─── Store function ────────────────────────────────────────────────────────────
def store_stock_data(df: pd.DataFrame, table_name: str = "stock_prices") -> None:
    """
    Saves cleaned DataFrame into the SQLite database.
    If the table already has data for this ticker, it replaces it.
    """
    ticker = df["ticker"].iloc[0]

    # Write data first — if_exists="append" creates the table if it doesn't exist
    # This means the table is guaranteed to exist before we try to delete from it
    df.to_sql(table_name, engine, if_exists="append", index=False)

    # Now remove any duplicate rows for this ticker that may exist from previous runs
    # We keep the most recently inserted rows using the rowid (SQLite internal ID)
    with engine.connect() as conn:
        conn.execute(
            __import__("sqlalchemy").text(f"""
                DELETE FROM {table_name}
                WHERE rowid NOT IN (
                    SELECT MAX(rowid)
                    FROM {table_name}
                    WHERE ticker = :t
                    GROUP BY date, ticker
                )
                AND ticker = :t
            """),
            {"t": ticker}
        )
        conn.commit()

    print(f"  Stored {len(df)} rows → table '{table_name}'")


# ─── Master pipeline function ──────────────────────────────────────────────────
def run_pipeline(ticker: str, period: str = "1y") -> pd.DataFrame:
    """
    Runs the full pipeline: fetch → clean → store.
    This is the one function you call from outside this file.

    Args:
        ticker: Stock symbol
        period: Time period

    Returns:
        The cleaned DataFrame (useful for immediate inspection)
    """
    raw      = fetch_stock_data(ticker, period)
    cleaned  = clean_stock_data(raw, ticker)
    store_stock_data(cleaned)
    print(f"Pipeline complete for {ticker}\n")
    return cleaned


# ─── Run directly ─────────────────────────────────────────────────────────────
# This block only runs when you execute THIS file directly
# It won't run when another file imports from this module
if __name__ == "__main__":

    # A basket of stocks across different sectors
    tickers = ["AAPL", "MSFT", "GOOGL", "TSLA", "JPM"]

    for t in tickers:
        run_pipeline(t, period="2y")

    print("All tickers loaded into database successfully.")
    