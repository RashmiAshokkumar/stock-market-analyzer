# src/screener.py

import yfinance as yf
import pandas as pd
import numpy as np

# ── Fetch fundamentals ─────────────────────────────────────────────────────────
def fetch_fundamentals(ticker: str) -> dict:
    """
    Fetches fundamental financial ratios from Yahoo Finance.
    These come from the company's actual financial statements.

    Returns a dictionary of key metrics.
    """
    stock = yf.Ticker(ticker)
    info  = stock.info  # Yahoo Finance metadata dictionary

    # Safely extract each field — use None if not available
    return {
        "ticker":            ticker.upper(),
        "company":           info.get("longName",             "N/A"),
        "sector":            info.get("sector",               "N/A"),
        "market_cap_B":      round(info.get("marketCap", 0) / 1e9, 2),
        "pe_ratio":          info.get("trailingPE",           None),
        "forward_pe":        info.get("forwardPE",            None),
        "price_to_book":     info.get("priceToBook",          None),
        "debt_to_equity":    info.get("debtToEquity",         None),
        "return_on_equity":  info.get("returnOnEquity",       None),
        "revenue_growth":    info.get("revenueGrowth",        None),
        "gross_margins":     info.get("grossMargins",         None),
        "current_price":     info.get("currentPrice",         None),
        "52w_high":          info.get("fiftyTwoWeekHigh",     None),
        "52w_low":           info.get("fiftyTwoWeekLow",      None),
    }


def build_fundamentals_df(tickers: list) -> pd.DataFrame:
    """
    Builds a DataFrame of fundamentals for multiple tickers.
    """
    records = []
    for ticker in tickers:
        print(f"Fetching fundamentals for {ticker}...")
        data = fetch_fundamentals(ticker)
        records.append(data)

    df = pd.DataFrame(records)

    # Convert ratios to percentages for readability
    for col in ["return_on_equity", "revenue_growth", "gross_margins"]:
        df[col] = (df[col] * 100).round(2)

    # Round other columns
    for col in ["pe_ratio", "forward_pe", "price_to_book", "debt_to_equity"]:
        df[col] = df[col].round(2)

    return df


# ── Screener logic ─────────────────────────────────────────────────────────────
def screen_stocks(df: pd.DataFrame, criteria: dict) -> pd.DataFrame:
    """
    Filters stocks based on user-defined fundamental criteria.

    Args:
        df:       DataFrame from build_fundamentals_df()
        criteria: Dictionary of column → (min, max) tuples
                  Use None for no limit on either side

    Example criteria:
        {
            "pe_ratio":         (None, 30),   # P/E below 30
            "return_on_equity": (15, None),   # ROE above 15%
            "debt_to_equity":   (None, 150),  # D/E below 150
        }
    """
    filtered = df.copy()

    for column, (min_val, max_val) in criteria.items():
        if column not in filtered.columns:
            continue
        if min_val is not None:
            filtered = filtered[filtered[column] >= min_val]
        if max_val is not None:
            filtered = filtered[filtered[column] <= max_val]

    return filtered


# ── 52-week position ───────────────────────────────────────────────────────────
def week52_position(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column showing where current price sits
    within the 52-week high/low range (like %B but for the year).

    0% = at 52-week low, 100% = at 52-week high
    """
    df = df.copy()
    df["52w_position_%"] = round(
        (df["current_price"] - df["52w_low"]) /
        (df["52w_high"] - df["52w_low"]) * 100, 1
    )
    return df


# ── Run directly ───────────────────────────────────────────────────────────────
if __name__ == "__main__":

    tickers = ["AAPL", "MSFT", "GOOGL", "TSLA", "JPM"]

    print("Fetching fundamental data...\n")
    df = build_fundamentals_df(tickers)
    df = week52_position(df)

    print("\n" + "=" * 65)
    print("FULL FUNDAMENTALS TABLE")
    print("=" * 65)
    display_cols = [
        "ticker", "sector", "market_cap_B", "pe_ratio",
        "forward_pe", "debt_to_equity", "return_on_equity",
        "revenue_growth", "52w_position_%"
    ]
    print(df[display_cols].to_string(index=False))

    print("\n" + "=" * 65)
    print("SCREENER — Value stocks: P/E < 30, ROE > 10%, D/E < 200")
    print("=" * 65)
    criteria = {
        "pe_ratio":         (None, 30),
        "return_on_equity": (10,   None),
        "debt_to_equity":   (None, 200),
    }
    screened = screen_stocks(df, criteria)
    print(screened[["ticker", "company", "pe_ratio",
                     "return_on_equity", "debt_to_equity"]].to_string(index=False))

    print("\n" + "=" * 65)
    print("SCREENER — Growth stocks: Revenue growth > 5%, Gross margin > 40%")
    print("=" * 65)
    growth_criteria = {
        "revenue_growth": (5,  None),
        "gross_margins":  (40, None),
    }
    growth = screen_stocks(df, growth_criteria)
    print(growth[["ticker", "company", "revenue_growth",
                   "gross_margins", "forward_pe"]].to_string(index=False))