import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
import os

# ── Database connection ────────────────────────────────────────────────────────
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH  = os.path.join(BASE_DIR, "database", "stocks.db")
engine   = create_engine(f"sqlite:///{DB_PATH}")


def load_prices(ticker: str) -> pd.DataFrame:
    """Loads close prices for a ticker from the database."""
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT date, close FROM stock_prices WHERE ticker = :t ORDER BY date"),
            conn,
            params={"t": ticker}
        )
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


# ── RSI ────────────────────────────────────────────────────────────────────────
def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """
    Calculates RSI (Relative Strength Index) from scratch using numpy.

    Steps:
    1. Calculate price change between each day
    2. Separate gains (positive changes) and losses (negative changes)
    3. Calculate average gain and average loss over 'period' days
    4. RS = avg_gain / avg_loss
    5. RSI = 100 - (100 / (1 + RS))

    Args:
        df:     DataFrame with 'close' column
        period: Lookback period (default 14 days — industry standard)
    """
    close = df["close"].values  # Convert to numpy array for fast math

    # Step 1: Daily price changes
    # np.diff gives the difference between consecutive elements
    # e.g. [100, 102, 101] → [2, -1]
    delta = np.diff(close)

    # Step 2: Separate gains and losses
    # np.where(condition, value_if_true, value_if_false)
    gains  = np.where(delta > 0, delta, 0)  # keep positive, zero out negatives
    losses = np.where(delta < 0, -delta, 0) # keep negative (as positive), zero out gains

    # Step 3: Calculate rolling average gain and loss
    # We use pandas rolling() here because numpy doesn't have built-in rolling
    gains_series  = pd.Series(gains)
    losses_series = pd.Series(losses)

    avg_gain = gains_series.rolling(window=period).mean()
    avg_loss = losses_series.rolling(window=period).mean()

    # Step 4: RS = average gain / average loss
    # We add a tiny number (1e-10) to avoid division by zero
    rs = avg_gain / (avg_loss + 1e-10)

    # Step 5: RSI formula
    rsi = 100 - (100 / (1 + rs))

    # Build result DataFrame
    # Note: np.diff reduces length by 1, so we align with original dates
    result = df.iloc[1:].copy()  # drop first row (no delta for it)
    result["rsi"] = rsi.values
    result["rsi"] = result["rsi"].round(2)

    # Add signal column — this is the interpretation layer
    result["rsi_signal"] = pd.cut(
        result["rsi"],
        bins=[0, 30, 70, 100],
        labels=["oversold", "neutral", "overbought"]
    )

    return result[["date", "close", "rsi", "rsi_signal"]]


# ── EMA helper ────────────────────────────────────────────────────────────────
def calculate_ema(prices: np.ndarray, period: int) -> np.ndarray:
    """
    Calculates Exponential Moving Average from scratch.

    EMA gives more weight to recent prices than older ones.
    Multiplier = 2 / (period + 1)
    EMA today = (Close today × multiplier) + (EMA yesterday × (1 - multiplier))

    Args:
        prices: numpy array of close prices
        period: EMA period (e.g. 12 or 26 for MACD)
    """
    multiplier = 2 / (period + 1)

    # Start the EMA with a simple average of the first 'period' prices
    ema = np.zeros(len(prices))
    ema[period - 1] = np.mean(prices[:period])  # seed value

    # Then apply EMA formula for every day after that
    for i in range(period, len(prices)):
        ema[i] = (prices[i] * multiplier) + (ema[i - 1] * (1 - multiplier))

    # Set values before the seed to NaN (not enough data yet)
    ema[:period - 1] = np.nan

    return ema


# ── MACD ──────────────────────────────────────────────────────────────────────
def calculate_macd(df: pd.DataFrame,
                   fast: int = 12,
                   slow: int = 26,
                   signal: int = 9) -> pd.DataFrame:
    """
    Calculates MACD from scratch using our custom EMA function.

    MACD line   = EMA(12) - EMA(26)
    Signal line = EMA(9) of the MACD line
    Histogram   = MACD line - Signal line
                  positive histogram = bullish momentum
                  negative histogram = bearish momentum

    Args:
        df:     DataFrame with 'close' column
        fast:   Fast EMA period (default 12)
        slow:   Slow EMA period (default 26)
        signal: Signal line period (default 9)
    """
    close = df["close"].values

    # Calculate both EMAs using our function
    ema_fast = calculate_ema(close, fast)
    ema_slow = calculate_ema(close, slow)

    # MACD line = difference between fast and slow EMA
    macd_line = ema_fast - ema_slow

    # Signal line = EMA of the MACD line itself
    # We only calculate EMA where MACD is valid (not NaN)
    valid_start = slow - 1  # first valid index
    signal_line = np.full(len(close), np.nan)
    macd_valid  = macd_line[valid_start:]
    signal_calc = calculate_ema(macd_valid, signal)
    signal_line[valid_start:] = signal_calc

    # Histogram = difference between MACD and signal
    histogram = macd_line - signal_line

    result = df.copy()
    result["ema_12"]     = np.round(ema_fast,   2)
    result["ema_26"]     = np.round(ema_slow,   2)
    result["macd"]       = np.round(macd_line,  4)
    result["signal"]     = np.round(signal_line,4)
    result["histogram"]  = np.round(histogram,  4)

    # Crossover signal — when MACD crosses above signal = bullish
    result["macd_signal"] = np.where(
        result["macd"] > result["signal"], "bullish", "bearish"
    )

    return result[["date", "close", "ema_12", "ema_26",
                   "macd", "signal", "histogram", "macd_signal"]].dropna()


# ── Bollinger Bands ───────────────────────────────────────────────────────────
def calculate_bollinger_bands(df: pd.DataFrame, period: int = 20,
                               num_std: float = 2.0) -> pd.DataFrame:
    """
    Calculates Bollinger Bands from scratch.

    Middle band = 20-day Simple Moving Average
    Upper band  = Middle band + (2 × standard deviation)
    Lower band  = Middle band - (2 × standard deviation)

    %B indicator shows WHERE price is within the bands:
    %B = (price - lower) / (upper - lower)
    %B above 1.0 = price above upper band (overbought)
    %B below 0.0 = price below lower band (oversold)

    Args:
        df:      DataFrame with 'close' column
        period:  Lookback window (default 20 days)
        num_std: Number of standard deviations (default 2)
    """
    close = df["close"]

    # Middle band = simple moving average
    middle = close.rolling(window=period).mean()

    # Standard deviation over same window
    std = close.rolling(window=period).std()

    # Upper and lower bands
    upper = middle + (num_std * std)
    lower = middle - (num_std * std)

    # %B indicator — where is price within the bands?
    percent_b = (close - lower) / (upper - lower)

    # Band width — measures how wide the bands are (volatility)
    bandwidth = (upper - lower) / middle * 100

    result = df.copy()
    result["bb_middle"]    = middle.round(2)
    result["bb_upper"]     = upper.round(2)
    result["bb_lower"]     = lower.round(2)
    result["bb_percent_b"] = percent_b.round(4)
    result["bb_bandwidth"] = bandwidth.round(2)

    # Signal: is price near the upper or lower band?
    result["bb_signal"] = np.where(
        result["bb_percent_b"] > 0.95, "near upper band",
        np.where(result["bb_percent_b"] < 0.05, "near lower band", "within bands")
    )

    return result[["date", "close", "bb_upper", "bb_middle",
                   "bb_lower", "bb_percent_b", "bb_bandwidth", "bb_signal"]].dropna()


# ── Combined analysis ─────────────────────────────────────────────────────────
def full_technical_analysis(ticker: str) -> pd.DataFrame:
    """
    Runs all three indicators and merges them into one DataFrame.
    This is what we'll use for the Excel report in Phase 6.
    """
    df = load_prices(ticker)

    rsi  = calculate_rsi(df)[["date", "rsi", "rsi_signal"]]
    macd = calculate_macd(df)[["date", "macd", "signal",
                                "histogram", "macd_signal"]]
    bb   = calculate_bollinger_bands(df)[["date", "bb_upper", "bb_middle",
                                          "bb_lower", "bb_percent_b",
                                          "bb_bandwidth", "bb_signal"]]

    # Merge all on date
    merged = df.merge(rsi,  on="date", how="left") \
               .merge(macd, on="date", how="left") \
               .merge(bb,   on="date", how="left")

    merged["ticker"] = ticker
    return merged


# ── Run directly ──────────────────────────────────────────────────────────────
if __name__ == "__main__":

    tickers = ["AAPL", "TSLA"]

    for ticker in tickers:
        print("=" * 65)
        print(f"  {ticker} — Technical Analysis (last 10 rows)")
        print("=" * 65)

        df = load_prices(ticker)

        print(f"\n--- RSI (period=14) ---")
        rsi_df = calculate_rsi(df)
        print(rsi_df.tail(10).to_string(index=False))

        print(f"\n--- MACD (12, 26, 9) ---")
        macd_df = calculate_macd(df)
        print(macd_df.tail(10)[["date","close","macd",
                                 "signal","histogram","macd_signal"]].to_string(index=False))

        print(f"\n--- Bollinger Bands (period=20) ---")
        bb_df = calculate_bollinger_bands(df)
        print(bb_df.tail(10)[["date","close","bb_upper",
                               "bb_lower","bb_percent_b","bb_signal"]].to_string(index=False))

        print()