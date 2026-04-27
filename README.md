\# Stock Market Analyzer



An end-to-end personal finance and stock market analysis tool built with Python and SQL.



\## What it does

\- Pulls live stock data for multiple tickers via the yfinance API

\- Stores 2 years of OHLCV data in a SQLite database

\- Runs SQL analysis using window functions (moving averages, returns, volatility)

\- Calculates RSI, MACD, and Bollinger Bands from scratch using numpy

\- Detects price and volume anomalies using Z-score and IQR methods

\- Screens stocks by fundamental ratios (P/E, ROE, debt-to-equity)

\- Generates a formatted Excel report automatically using openpyxl



\## Tech Stack

Python · SQL (SQLite) · pandas · numpy · yfinance · SQLAlchemy · openpyxl



\## Project Structure

stock-market-analyzer/

├── data/               # Raw and processed data files

├── database/           # SQLite database (generated, not tracked)

├── notebooks/          # Jupyter notebooks for exploration

├── reports/            # Generated Excel reports (not tracked)

├── src/

│   ├── data\_ingestion.py       # Fetch and store stock data

│   ├── sql\_analysis.py         # Window function queries

│   ├── technical\_analysis.py   # RSI, MACD, Bollinger Bands

│   ├── anomaly\_detection.py    # Z-score and IQR anomaly detection

│   ├── screener.py             # Fundamental stock screener

│   └── report\_generator.py     # Automated Excel report

└── requirements.txt



\## Setup

```bash

git clone https://github.com/YourUsername/stock-market-analyzer.git

cd stock-market-analyzer

python -m venv venv

venv\\Scripts\\activate

pip install -r requirements.txt

```



\## Usage

```bash

\# Step 1 - Load stock data into database

python src/data\_ingestion.py



\# Step 2 - Run SQL analysis

python src/sql\_analysis.py



\# Step 3 - Calculate technical indicators

python src/technical\_analysis.py



\# Step 4 - Detect anomalies and screen stocks

python src/anomaly\_detection.py

python src/screener.py



\# Step 5 - Generate Excel report

python src/report\_generator.py

```



\## Skills Demonstrated

\- Python (pandas, numpy, sqlalchemy, openpyxl)

\- SQL window functions (LAG, RANK, FIRST\_VALUE, rolling aggregations)

\- Financial analysis (OHLCV data, technical indicators, fundamental ratios)

\- Statistical methods (Z-score, IQR anomaly detection)

\- Data pipeline design (fetch → clean → store → analyse → report)

