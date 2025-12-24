#!/usr/bin/env python3
"""
Check database data for specific stocks.
"""

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.db_connection import get_connection

load_dotenv()


def check_stock_data(tickers: list):
    """Check all database data for given tickers."""
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        for ticker in tickers:
            print("=" * 70)
            print(f"📊 DATA FOR {ticker}")
            print("=" * 70)
            print()
            
            # 1. Company Profile
            cursor.execute('''
                SELECT name, sector, industry, market_cap, exchange, price, employees
                FROM company_profiles
                WHERE ticker = %s
            ''', (ticker,))
            
            profile = cursor.fetchone()
            if profile:
                name, sector, industry, market_cap, exchange, price, employees = profile
                print("🏢 Company Profile:")
                print(f"   Name: {name}")
                print(f"   Sector: {sector}")
                print(f"   Industry: {industry}")
                print(f"   Market Cap: ${market_cap:,.0f}" if market_cap else "   Market Cap: N/A")
                print(f"   Exchange: {exchange}")
                print(f"   Current Price: ${price:.2f}" if price else "   Current Price: N/A")
                print(f"   Employees: {employees:,}" if employees else "   Employees: N/A")
            else:
                print("🏢 Company Profile: ❌ Not found")
            print()
            
            # 2. Price Data
            cursor.execute('''
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN close > 0 THEN 1 END) as valid,
                    MIN(date) as earliest,
                    MAX(date) as latest,
                    MIN(close) as min_price,
                    MAX(close) as max_price,
                    AVG(close) as avg_price
                FROM stock_prices
                WHERE ticker = %s
            ''', (ticker,))
            
            price_stats = cursor.fetchone()
            if price_stats:
                total, valid, earliest, latest, min_price, max_price, avg_price = price_stats
                print("📈 Price Data:")
                print(f"   Total records: {total:,}")
                print(f"   Valid prices (close > 0): {valid:,} ({valid/total*100:.1f}%)")
                if earliest and latest:
                    years = (latest - earliest).days / 365.25
                    print(f"   Date range: {earliest} to {latest} ({years:.1f} years)")
                if min_price and max_price:
                    print(f"   Price range: ${min_price:.2f} - ${max_price:.2f}")
                    print(f"   Average price: ${avg_price:.2f}")
                
                # Sample recent prices
                cursor.execute('''
                    SELECT date, open, high, low, close, volume, change, change_percent, vwap, index_name
                    FROM stock_prices
                    WHERE ticker = %s AND close > 0
                    ORDER BY date DESC
                    LIMIT 5
                ''', (ticker,))
                
                recent = cursor.fetchall()
                if recent:
                    print()
                    print("   Recent Prices (last 5 days):")
                    for date, open_price, high, low, close, volume, change, change_pct, vwap, index_name in recent:
                        change_str = f"{change:+.2f}" if change else "N/A"
                        change_pct_str = f"{change_pct:+.2f}%" if change_pct else "N/A"
                        print(f"     {date}: O=${open_price:.2f}, H=${high:.2f}, L=${low:.2f}, C=${close:.2f}, V={volume:,}, Change={change_str} ({change_pct_str}), VWAP=${vwap:.2f}, Index={index_name}")
            else:
                print("📈 Price Data: ❌ No price data found")
            print()
            
            # 3. Financial Statements
            cursor.execute('SELECT COUNT(*) FROM income_statements WHERE ticker = %s', (ticker,))
            income_count = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM balance_sheets WHERE ticker = %s', (ticker,))
            balance_count = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM cash_flow_statements WHERE ticker = %s', (ticker,))
            cashflow_count = cursor.fetchone()[0]
            
            print("💰 Financial Statements:")
            print(f"   Income Statements: {income_count}")
            print(f"   Balance Sheets: {balance_count}")
            print(f"   Cash Flow Statements: {cashflow_count}")
            
            if income_count > 0:
                cursor.execute('''
                    SELECT date, period, revenue, net_income, eps
                    FROM income_statements
                    WHERE ticker = %s
                    ORDER BY date DESC
                    LIMIT 3
                ''', (ticker,))
                
                recent_income = cursor.fetchall()
                if recent_income:
                    print()
                    print("   Recent Income Statements:")
                    for date, period, revenue, net_income, eps in recent_income:
                        print(f"     {date} ({period}): Revenue=${revenue:,.0f}, Net Income=${net_income:,.0f}, EPS=${eps:.2f}")
            print()
            
            # 4. Financial Metrics
            cursor.execute('''
                SELECT COUNT(DISTINCT metric_name) as unique_metrics,
                       COUNT(*) as total_metrics
                FROM financial_metrics
                WHERE ticker = %s
            ''', (ticker,))
            
            metric_stats = cursor.fetchone()
            if metric_stats:
                unique_metrics, total_metrics = metric_stats
                print("📊 Financial Metrics:")
                print(f"   Unique metrics: {unique_metrics}")
                print(f"   Total metric records: {total_metrics}")
                
                # Sample metrics
                cursor.execute('''
                    SELECT metric_name, metric_value, metric_unit, period_end_date
                    FROM financial_metrics
                    WHERE ticker = %s
                    ORDER BY period_end_date DESC
                    LIMIT 10
                ''', (ticker,))
                
                sample_metrics = cursor.fetchall()
                if sample_metrics:
                    print()
                    print("   Sample Metrics:")
                    for metric_name, value, unit, period in sample_metrics:
                        print(f"     {metric_name}: {value} {unit} (as of {period})")
            else:
                print("📊 Financial Metrics: ❌ No metrics found")
            print()
            
            # 5. News Articles
            cursor.execute('''
                SELECT COUNT(*), MIN(published_at), MAX(published_at)
                FROM news_articles
                WHERE ticker = %s
            ''', (ticker,))
            
            news_stats = cursor.fetchone()
            if news_stats:
                news_count, earliest_news, latest_news = news_stats
                print("📰 News Articles:")
                print(f"   Total articles: {news_count}")
                if earliest_news and latest_news:
                    print(f"   Date range: {earliest_news} to {latest_news}")
            else:
                print("📰 News Articles: ❌ No news found")
            print()
            
            # 6. DCF Valuation
            cursor.execute('''
                SELECT dcf_value, stock_price, upside_percent, date
                FROM dcf_valuations
                WHERE ticker = %s
                ORDER BY date DESC
                LIMIT 1
            ''', (ticker,))
            
            dcf = cursor.fetchone()
            if dcf:
                dcf_value, stock_price, upside, date = dcf
                print("💎 DCF Valuation:")
                print(f"   DCF Value: ${dcf_value:.2f}")
                print(f"   Stock Price: ${stock_price:.2f}")
                print(f"   Upside: {upside:.2f}%")
                print(f"   Date: {date}")
            else:
                print("💎 DCF Valuation: ❌ Not found")
            print()
            
            print()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Check database data for stocks")
    parser.add_argument("tickers", nargs="+", help="Stock tickers to check")
    
    args = parser.parse_args()
    
    check_stock_data([t.upper() for t in args.tickers])

