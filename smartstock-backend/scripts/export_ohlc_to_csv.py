#!/usr/bin/env python3
"""
Export OHLC data to CSV files.
Exports Nasdaq 100 and S&P 500 stocks separately.
"""

import sys
import csv
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.db_connection import get_connection

load_dotenv()

# Nasdaq 100 tickers (as of late 2024/early 2025)
NASDAQ_100 = [
    "AAPL", "MSFT", "AMZN", "NVDA", "META", "GOOGL", "GOOG", "TSLA", "AVGO", "COST",
    "ASML", "AZN", "LIN", "AMD", "ADBE", "PEP", "TMUS", "NFLX", "CSCO", "INTU",
    "CMCSA", "AMAT", "QCOM", "AMGN", "ISRG", "TXN", "HON", "BKNG", "VRTX", "ARM",
    "REGN", "PANW", "MU", "ADP", "LRCX", "ADI", "MDLZ", "KLAC", "PDD", "MELI",
    "INTC", "SNPS", "CDNS", "CSX", "PYPL", "CRWD", "MAR", "ORLY", "CTAS", "WDAY",
    "NXPI", "ROP", "ADSK", "MNST", "TEAM", "DXCM", "PCAR", "ROST", "IDXX", "PH",
    "KDP", "CPRT", "LULU", "PAYX", "AEP", "ODFL", "FAST", "GEHC", "MCHP", "CSGP",
    "EXC", "ON", "BKR", "CTSH", "ABNB", "CDW", "FANG", "MDB", "TTD", "ANSS",
    "CEG", "DDOG", "ZS", "ILMN", "DLTR", "WBD", "WBA", "EBAY"
]

# S&P 500 - we'll get from database or use a comprehensive list
SP_500_TOP = [
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "GOOG", "META", "BRK.B", "TSLA", "UNH",
    "JPM", "LLY", "JNJ", "V", "XOM", "MA", "AVGO", "PG", "HD", "CVX",
    "ABBV", "ADBE", "COST", "KO", "PEP", "ORCL", "BAC", "WMT", "CSCO", "CRM",
    "MCD", "ACN", "ABT", "LIN", "PM", "TMO", "VZ", "DIS", "INTU", "TXN",
    "DHR", "NEE", "PFE", "NKE", "CMCSA", "WFC", "AMGN", "LOW", "UNP", "IBM",
    "COP", "MS", "HON", "BA", "UPS", "INTC", "BMY", "RTX", "CAT", "GE",
    "SBUX", "AMAT", "DE", "PLD", "GS", "ISRG", "BLK", "NOW", "MDLZ", "TJX",
    "GILD", "AXP", "AMT", "LMT", "EL", "ADP", "SYK", "C", "CVS", "ADI",
    "MMC", "ZTS", "CB", "REGN", "MDT", "VRTX", "CI", "MO", "SCHW", "LRCX",
    "BDX", "DUK", "BSX", "EW", "HUM", "DELL", "BX", "SNPS", "CDNS", "ETN"
]


def export_ohlc_to_csv(tickers: list, filename: str, output_dir: Path):
    """Export OHLC data for given tickers to CSV."""
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / filename
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Get all OHLC data for these tickers
        placeholders = ','.join(['%s'] * len(tickers))
        cursor.execute(f'''
            SELECT ticker, date, open, high, low, close, volume, change, change_percent, vwap, index_name
            FROM stock_prices
            WHERE ticker IN ({placeholders})
            AND close > 0
            ORDER BY ticker ASC, date DESC
        ''', tickers)
        
        rows = cursor.fetchall()
        
        if not rows:
            print(f"⚠️  No data found for tickers: {', '.join(tickers[:10])}...")
            return 0
        
        # Write to CSV
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            # Header
            writer.writerow(['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adjusted_Close'])
            
            # Data rows
            for row in rows:
                ticker, date, open_price, high, low, close, volume, adj_close = row
                writer.writerow([
                    ticker,
                    date.strftime('%Y-%m-%d') if date else '',
                    f'{open_price:.2f}' if open_price else '',
                    f'{high:.2f}' if high else '',
                    f'{low:.2f}' if low else '',
                    f'{close:.2f}' if close else '',
                    int(volume) if volume else '',
                    f'{adj_close:.2f}' if adj_close else ''
                ])
        
        print(f"✅ Exported {len(rows):,} records to {filepath}")
        return len(rows)


def get_sp500_tickers_from_db():
    """Get S&P 500 tickers from database (stocks with company profiles)."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT ticker FROM company_profiles ORDER BY ticker')
        return [row[0] for row in cursor.fetchall()]


def main():
    """Export Nasdaq 100 and S&P 500 OHLC data to CSV."""
    # Output directory (will be gitignored)
    output_dir = Path(__file__).parent.parent / 'data' / 'exports'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 70)
    print("📊 EXPORTING OHLC DATA TO CSV")
    print("=" * 70)
    print()
    print(f"Output directory: {output_dir}")
    print()
    
    # 1. Export Nasdaq 100
    print("1. Exporting Nasdaq 100 stocks...")
    nasdaq_tickers = [t for t in NASDAQ_100 if t]
    nasdaq_count = export_ohlc_to_csv(
        nasdaq_tickers,
        f'nasdaq100_ohlc_{datetime.now().strftime("%Y%m%d")}.csv',
        output_dir
    )
    print()
    
    # 2. Export S&P 500 (get from database)
    print("2. Exporting S&P 500 stocks...")
    sp500_tickers = get_sp500_tickers_from_db()
    sp500_count = export_ohlc_to_csv(
        sp500_tickers,
        f'sp500_ohlc_{datetime.now().strftime("%Y%m%d")}.csv',
        output_dir
    )
    print()
    
    print("=" * 70)
    print("✨ EXPORT COMPLETE")
    print("=" * 70)
    print(f"Nasdaq 100: {nasdaq_count:,} records")
    print(f"S&P 500: {sp500_count:,} records")
    print()
    print(f"Files saved to: {output_dir}")
    print("  - nasdaq100_ohlc_YYYYMMDD.csv")
    print("  - sp500_ohlc_YYYYMMDD.csv")


if __name__ == "__main__":
    main()

