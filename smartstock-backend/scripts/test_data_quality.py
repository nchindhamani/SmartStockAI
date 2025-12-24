#!/usr/bin/env python3
"""
Comprehensive Data Quality Testing Script
Tests ingested data for completeness, accuracy, and cleanliness.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.db_connection import get_connection
from data.metrics_store import get_metrics_store
from data.news_store import get_news_store
from data.financial_statements_store import get_financial_statements_store

load_dotenv()


class DataQualityTester:
    """Comprehensive data quality testing."""
    
    def __init__(self):
        self.issues = []
        self.warnings = []
        self.stats = {}
    
    def test_stock_completeness(self, ticker: str) -> Dict[str, Any]:
        """Test if a stock has all required data."""
        results = {
            "ticker": ticker,
            "has_profile": False,
            "has_prices": False,
            "price_count": 0,
            "price_years": 0,
            "has_statements": False,
            "income_count": 0,
            "balance_count": 0,
            "cashflow_count": 0,
            "has_metrics": False,
            "metrics_count": 0,
            "has_news": False,
            "news_count": 0,
            "has_dcf": False,
            "issues": []
        }
        
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Company Profile
            cursor.execute('SELECT COUNT(*) FROM company_profiles WHERE ticker = %s', (ticker,))
            if cursor.fetchone()[0] > 0:
                results["has_profile"] = True
            else:
                results["issues"].append("Missing company profile")
            
            # Price Data
            cursor.execute('''
                SELECT COUNT(*), MIN(date), MAX(date)
                FROM stock_prices 
                WHERE ticker = %s
            ''', (ticker,))
            price_row = cursor.fetchone()
            if price_row and price_row[0] > 0:
                results["has_prices"] = True
                results["price_count"] = price_row[0]
                if price_row[1] and price_row[2]:
                    years = (price_row[2] - price_row[1]).days / 365.25
                    results["price_years"] = years
                    if years < 4.5:
                        results["issues"].append(f"Only {years:.1f} years of price data (expected ~5 years)")
            else:
                results["issues"].append("Missing price data")
            
            # Financial Statements
            cursor.execute('SELECT COUNT(*) FROM income_statements WHERE ticker = %s', (ticker,))
            results["income_count"] = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(*) FROM balance_sheets WHERE ticker = %s', (ticker,))
            results["balance_count"] = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(*) FROM cash_flow_statements WHERE ticker = %s', (ticker,))
            results["cashflow_count"] = cursor.fetchone()[0]
            
            if results["income_count"] > 0 or results["balance_count"] > 0 or results["cashflow_count"] > 0:
                results["has_statements"] = True
            else:
                results["issues"].append("Missing financial statements")
            
            if results["income_count"] < 10:
                results["issues"].append(f"Only {results['income_count']} income statements (expected ~20)")
            
            # Financial Metrics
            cursor.execute('SELECT COUNT(*) FROM financial_metrics WHERE ticker = %s', (ticker,))
            results["metrics_count"] = cursor.fetchone()[0]
            if results["metrics_count"] > 0:
                results["has_metrics"] = True
            else:
                results["warnings"].append("No financial metrics")
            
            # News
            cursor.execute('SELECT COUNT(*) FROM news_articles WHERE ticker = %s', (ticker,))
            results["news_count"] = cursor.fetchone()[0]
            if results["news_count"] > 0:
                results["has_news"] = True
            
            # DCF Valuation
            cursor.execute('SELECT COUNT(*) FROM dcf_valuations WHERE ticker = %s', (ticker,))
            if cursor.fetchone()[0] > 0:
                results["has_dcf"] = True
        
        return results
    
    def test_data_accuracy(self, ticker: str) -> Dict[str, Any]:
        """Test data accuracy and consistency."""
        results = {
            "ticker": ticker,
            "price_consistency": True,
            "metric_consistency": True,
            "date_consistency": True,
            "issues": []
        }
        
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Check price data consistency
            cursor.execute('''
                SELECT date, open, high, low, close
                FROM stock_prices
                WHERE ticker = %s
                AND (high < low OR close > high OR close < low OR open > high OR open < low)
                LIMIT 10
            ''', (ticker,))
            invalid_prices = cursor.fetchall()
            if invalid_prices:
                results["price_consistency"] = False
                results["issues"].append(f"{len(invalid_prices)} price records with invalid OHLC values")
            
            # Check for null/zero prices (volume = 0 is valid for holidays/weekends)
            cursor.execute('''
                SELECT COUNT(*) FROM stock_prices
                WHERE ticker = %s
                AND (close IS NULL OR close = 0)
            ''', (ticker,))
            null_prices = cursor.fetchone()[0]
            if null_prices > 0:
                results["issues"].append(f"{null_prices} price records with null/zero close prices")
            
            # Check date consistency (no future dates)
            cursor.execute('''
                SELECT COUNT(*) FROM stock_prices
                WHERE ticker = %s
                AND date > CURRENT_DATE
            ''', (ticker,))
            future_dates = cursor.fetchone()[0]
            if future_dates > 0:
                results["date_consistency"] = False
                results["issues"].append(f"{future_dates} price records with future dates")
            
            # Check for duplicate dates
            cursor.execute('''
                SELECT date, COUNT(*) as cnt
                FROM stock_prices
                WHERE ticker = %s
                GROUP BY date
                HAVING COUNT(*) > 1
                LIMIT 10
            ''', (ticker,))
            duplicates = cursor.fetchall()
            if duplicates:
                results["issues"].append(f"{len(duplicates)} duplicate price dates found")
        
        return results
    
    def test_data_cleanliness(self, ticker: str) -> Dict[str, Any]:
        """Test data cleanliness (no garbage data)."""
        results = {
            "ticker": ticker,
            "clean": True,
            "issues": []
        }
        
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Check for extremely large/small values (likely errors)
            # Note: Large-cap stocks can have volumes > 1T, so we check for truly extreme values
            cursor.execute('''
                SELECT COUNT(*) FROM stock_prices
                WHERE ticker = %s
                AND (close > 100000 OR close < 0.01 OR volume > 10000000000000)
            ''', (ticker,))
            extreme_values = cursor.fetchone()[0]
            if extreme_values > 0:
                results["clean"] = False
                results["issues"].append(f"{extreme_values} price records with extreme values")
            
            # Check company profile for required fields
            cursor.execute('''
                SELECT name, sector, industry
                FROM company_profiles
                WHERE ticker = %s
                LIMIT 1
            ''', (ticker,))
            profile = cursor.fetchone()
            if profile:
                name, sector, industry = profile
                if not name or name.strip() == "":
                    results["issues"].append("Company profile missing name")
                if not sector or sector.strip() == "":
                    results["warnings"].append("Company profile missing sector")
        
        return results
    
    def run_comprehensive_test(self, sample_tickers: List[str] = None, test_all: bool = False):
        """Run comprehensive data quality tests."""
        print("=" * 70)
        print("📊 COMPREHENSIVE DATA QUALITY TEST")
        print("=" * 70)
        print()
        
        if test_all:
            with get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT DISTINCT ticker FROM company_profiles ORDER BY ticker')
                all_tickers = [row[0] for row in cursor.fetchall()]
                sample_tickers = all_tickers[:50]  # Test first 50 for performance
                print(f"Testing {len(sample_tickers)} stocks (sample of all)...")
        elif sample_tickers is None:
            sample_tickers = ["AAPL", "MSFT", "GOOGL", "NVDA", "META", "TSLA", "AMZN", "JPM", "V", "JNJ"]
            print(f"Testing {len(sample_tickers)} sample stocks...")
        else:
            print(f"Testing {len(sample_tickers)} specified stocks...")
        
        print()
        
        completeness_results = []
        accuracy_results = []
        cleanliness_results = []
        
        for ticker in sample_tickers:
            print(f"Testing {ticker}...", end=" ")
            
            # Run all tests
            completeness = self.test_stock_completeness(ticker)
            accuracy = self.test_data_accuracy(ticker)
            cleanliness = self.test_data_cleanliness(ticker)
            
            completeness_results.append(completeness)
            accuracy_results.append(accuracy)
            cleanliness_results.append(cleanliness)
            
            # Quick status
            if completeness["issues"] or accuracy["issues"] or cleanliness["issues"]:
                print("⚠️  Issues found")
            else:
                print("✓ OK")
        
        print()
        print("=" * 70)
        print("📈 COMPLETENESS RESULTS")
        print("=" * 70)
        
        # Aggregate completeness stats
        has_all_data = sum(1 for r in completeness_results 
                          if r["has_profile"] and r["has_prices"] and r["has_statements"])
        avg_price_count = sum(r["price_count"] for r in completeness_results) / len(completeness_results) if completeness_results else 0
        avg_price_years = sum(r["price_years"] for r in completeness_results) / len(completeness_results) if completeness_results else 0
        
        print(f"Stocks with complete data: {has_all_data}/{len(completeness_results)} ({has_all_data/len(completeness_results)*100:.1f}%)")
        print(f"Average price records per stock: {avg_price_count:.0f}")
        print(f"Average years of price data: {avg_price_years:.1f}")
        print()
        
        # Show stocks with issues
        stocks_with_issues = [r for r in completeness_results if r["issues"]]
        if stocks_with_issues:
            print(f"⚠️  {len(stocks_with_issues)} stocks with completeness issues:")
            for r in stocks_with_issues[:10]:
                print(f"  {r['ticker']}: {', '.join(r['issues'][:3])}")
        
        print()
        print("=" * 70)
        print("✅ ACCURACY RESULTS")
        print("=" * 70)
        
        # Aggregate accuracy stats
        price_consistent = sum(1 for r in accuracy_results if r["price_consistency"])
        date_consistent = sum(1 for r in accuracy_results if r["date_consistency"])
        stocks_with_accuracy_issues = [r for r in accuracy_results if r["issues"]]
        
        print(f"Stocks with consistent price data: {price_consistent}/{len(accuracy_results)} ({price_consistent/len(accuracy_results)*100:.1f}%)")
        print(f"Stocks with consistent dates: {date_consistent}/{len(accuracy_results)} ({date_consistent/len(accuracy_results)*100:.1f}%)")
        
        if stocks_with_accuracy_issues:
            print(f"⚠️  {len(stocks_with_accuracy_issues)} stocks with accuracy issues:")
            for r in stocks_with_accuracy_issues[:10]:
                print(f"  {r['ticker']}: {', '.join(r['issues'][:2])}")
        
        print()
        print("=" * 70)
        print("🧹 CLEANLINESS RESULTS")
        print("=" * 70)
        
        clean_stocks = sum(1 for r in cleanliness_results if r["clean"] and not r["issues"])
        stocks_with_cleanliness_issues = [r for r in cleanliness_results if r["issues"]]
        
        print(f"Clean stocks (no issues): {clean_stocks}/{len(cleanliness_results)} ({clean_stocks/len(cleanliness_results)*100:.1f}%)")
        
        if stocks_with_cleanliness_issues:
            print(f"⚠️  {len(stocks_with_cleanliness_issues)} stocks with cleanliness issues:")
            for r in stocks_with_cleanliness_issues[:10]:
                print(f"  {r['ticker']}: {', '.join(r['issues'][:2])}")
        
        print()
        print("=" * 70)
        print("📊 OVERALL SUMMARY")
        print("=" * 70)
        
        total_issues = (len(stocks_with_issues) + len(stocks_with_accuracy_issues) + 
                       len(stocks_with_cleanliness_issues))
        total_stocks = len(completeness_results)
        
        print(f"Total stocks tested: {total_stocks}")
        print(f"Stocks with issues: {total_issues}")
        print(f"Data quality score: {(total_stocks - total_issues) / total_stocks * 100:.1f}%")
        print()
        
        if total_issues == 0:
            print("🎉 All tests passed! Data is clean and accurate.")
        elif total_issues < total_stocks * 0.1:
            print("✅ Data quality is good (less than 10% have issues)")
        else:
            print("⚠️  Data quality needs attention (more than 10% have issues)")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test data quality for ingested stocks")
    parser.add_argument("--tickers", nargs="+", help="Specific tickers to test")
    parser.add_argument("--all", action="store_true", help="Test all stocks (sample of 50)")
    parser.add_argument("--sample", type=int, default=10, help="Number of sample stocks to test (default: 10)")
    
    args = parser.parse_args()
    
    tester = DataQualityTester()
    
    if args.tickers:
        tester.run_comprehensive_test(sample_tickers=args.tickers)
    elif args.all:
        tester.run_comprehensive_test(test_all=True)
    else:
        # Default: test a sample
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT DISTINCT ticker FROM company_profiles ORDER BY ticker LIMIT %s', (args.sample,))
            sample = [row[0] for row in cursor.fetchall()]
        tester.run_comprehensive_test(sample_tickers=sample)

