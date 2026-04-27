import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
import os

# ── Database connection ────────────────────────────────────────────────────────
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH  = os.path.join(BASE_DIR, "database", "stocks.db")
engine   = create_engine(f"sqlite:///{DB_PATH}")


def load_prices(ticker: str) -> pd.DataFrame:
    """Loads full price + volume data for a ticker."""
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT date, open, high, low, close, volume
                FROM stock_prices
                WHERE ticker = :t
                ORDER BY date
            """),
            conn, params={"t": ticker}
        )
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Calculate daily return % — needed for anomaly detection
    df["daily_return"] = df["close"].pct_change() * 100
    df["ticker"] = ticker
    return df


# ── Z-score method ─────────────────────────────────────────────────────────────
def zscore_anomalies(df: pd.DataFrame, column: str,
                     threshold: float = 2.5) -> pd.DataFrame:
    """
    Flags rows where the Z-score of a column exceeds the threshold.

    Z-score = (value - mean) / standard_deviation
    It measures how many standard deviations a value is from the mean.
    A Z-score above 2.5 means the value is unusually high or low.

    Args:
        df:        DataFrame with stock data
        column:    Which column to check ('daily_return' or 'volume')
        threshold: How many std devs = anomaly (default 2.5)
    """
    mean = df[column].mean()
    std  = df[column].std()

    # Calculate Z-score for every row
    df = df.copy()
    df[f"zscore_{column}"] = (df[column] - mean) / std

    # Flag rows where absolute Z-score exceeds threshold
    df[f"anomaly_zscore_{column}"] = df[f"zscore_{column}"].abs() > threshold

    return df


# ── IQR method ─────────────────────────────────────────────────────────────────
def iqr_anomalies(df: pd.DataFrame, column: str,
                  multiplier: float = 1.5) -> pd.DataFrame:
    """
    Flags rows where a value falls outside the IQR fences.

    IQR = Q3 - Q1  (the middle 50% of data)
    Lower fence = Q1 - (1.5 × IQR)
    Upper fence = Q3 + (1.5 × IQR)
    Anything outside the fences = outlier

    Args:
        df:         DataFrame with stock data
        column:     Which column to check
        multiplier: How strict the fences are (default 1.5 = standard)
    """
    df = df.copy()
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1

    lower_fence = Q1 - (multiplier * IQR)
    upper_fence = Q3 + (multiplier * IQR)

    df[f"iqr_lower_{column}"] = round(lower_fence, 4)
    df[f"iqr_upper_{column}"] = round(upper_fence, 4)
    df[f"anomaly_iqr_{column}"] = (
        (df[column] < lower_fence) | (df[column] > upper_fence)
    )

    return df


# ── Combined anomaly detection ─────────────────────────────────────────────────
def detect_anomalies(ticker: str) -> pd.DataFrame:
    """
    Runs both Z-score and IQR anomaly detection on price returns
    and volume. Flags a row as a confirmed anomaly if EITHER
    method catches it.

    Returns only the anomalous rows for easy inspection.
    """
    df = load_prices(ticker).dropna(subset=["daily_return"])

    # Apply Z-score to daily returns and volume
    df = zscore_anomalies(df, "daily_return", threshold=2.5)
    df = zscore_anomalies(df, "volume",       threshold=2.5)

    # Apply IQR to daily returns and volume
    df = iqr_anomalies(df, "daily_return", multiplier=1.5)
    df = iqr_anomalies(df, "volume",       multiplier=1.5)

    # Confirmed anomaly = flagged by at least one method on returns OR volume
    df["is_anomaly"] = (
        df["anomaly_zscore_daily_return"] |
        df["anomaly_iqr_daily_return"]    |
        df["anomaly_zscore_volume"]       |
        df["anomaly_iqr_volume"]
    )

    # Severity label based on Z-score magnitude
    df["severity"] = pd.cut(
        df["zscore_daily_return"].abs(),
        bins=[0, 2.5, 3.5, np.inf],
        labels=["normal", "moderate", "extreme"]
    )

    # Return only anomalous rows with clean columns
    anomalies = df[df["is_anomaly"]].copy()
    anomalies = anomalies[[
        "date", "ticker", "close", "daily_return",
        "volume", "zscore_daily_return", "severity"
    ]].round(4)

    anomalies = anomalies.sort_values("date")
    return anomalies


# ── Volume spike detector ──────────────────────────────────────────────────────
def volume_spikes(ticker: str, top_n: int = 10) -> pd.DataFrame:
    """
    Finds the days with the highest unusual volume.
    High volume + big price move = significant event (earnings, news).
    High volume + small price move = institutional accumulation/distribution.

    Args:
        ticker: Stock symbol
        top_n:  How many top spikes to return
    """
    df = load_prices(ticker).dropna(subset=["daily_return"])

    avg_volume = df["volume"].mean()

    df["volume_ratio"] = df["volume"] / avg_volume  # e.g. 2.5 = 2.5x normal volume
    df["abs_return"]   = df["daily_return"].abs()

    # Classify event type based on volume and price move
    df["event_type"] = np.where(
        (df["volume_ratio"] > 1.5) & (df["abs_return"] > 2),
        "high volume + big move (news/earnings)",
        np.where(
            (df["volume_ratio"] > 1.5) & (df["abs_return"] <= 2),
            "high volume + small move (institutional)",
            "normal"
        )
    )

    spikes = df[df["volume_ratio"] > 1.5].copy()
    spikes = spikes[[
        "date", "ticker", "close", "daily_return",
        "volume", "volume_ratio", "event_type"
    ]].round(4)

    return spikes.nlargest(top_n, "volume_ratio")


# ── Run directly ───────────────────────────────────────────────────────────────
if __name__ == "__main__":

    tickers = ["AAPL", "TSLA", "MSFT"]

    for ticker in tickers:
        print("=" * 65)
        print(f"  {ticker} — Anomaly Detection")
        print("=" * 65)

        anomalies = detect_anomalies(ticker)
        print(f"\nPrice/Volume anomalies found: {len(anomalies)}")
        print(anomalies.tail(8).to_string(index=False))

        print(f"\nTop 5 volume spikes:")
        spikes = volume_spikes(ticker, top_n=5)
        print(spikes.to_string(index=False))
        print()