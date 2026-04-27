from sqlalchemy import create_engine, text

engine = create_engine('sqlite:///database/stocks.db')

with engine.connect() as conn:
    result = conn.execute(text('''
        SELECT ticker, COUNT(*) as rows, MIN(date) as from_date, MAX(date) as to_date
        FROM stock_prices
        GROUP BY ticker
        ORDER BY ticker
    '''))
    for row in result:
        print(row)