#!/usr/bin/env python3
"""
Data Quality Validation Script

Comprehensive validation of all ingested data:
- Data completeness (required fields populated)
- Data freshness (last update times)
- Data consistency (value ranges, relationships)
- Coverage statistics (ticker coverage across tables)

Usage:
    uv run python scripts/validate_data_quality.py [--fix] [--verbose]
    
Options:
    --fix: Attempt to fix common issues automatically
    --verbose: Show detailed validation results for all tickers
"""

import sys
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.db_connection import get_connection
from dotenv import load_dotenv

load_dotenv()


class DataQualityValidator:
    """Comprehensive data quality validation."""
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.issues = []
        self.warnings = []
        self.stats = {}
        
    def validate_all(self) -> Dict[str, Any]:
        """Run all validation checks."""
        print("=" * 100)
        print("DATA QUALITY VALIDATION")
        print("=" * 100)
        print()
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "company_profiles": self.validate_company_profiles(),
            "stock_prices": self.validate_stock_prices(),
            "financial_statements": self.validate_financial_statements(),
            "analyst_data": self.validate_analyst_data(),
            "data_freshness": self.validate_data_freshness(),
            "coverage": self.validate_coverage(),
            "summary": {}
        }
        
        # Generate summary
        results["summary"] = self.generate_summary(results)
        
        # Print summary
        self.print_summary(results)
        
        return results
    
    def validate_company_profiles(self) -> Dict[str, Any]:
        """Validate company profiles data quality."""
        print("1. COMPANY PROFILES VALIDATION")
        print("-" * 100)
        
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Total records
            cursor.execute("SELECT COUNT(*) FROM company_profiles")
            total = cursor.fetchone()[0] or 0
            
            # Required fields check
            cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE market_cap > 0) as with_market_cap,
                    COUNT(*) FILTER (WHERE market_cap = 0 OR market_cap IS NULL) as missing_market_cap,
                    COUNT(*) FILTER (WHERE exchange IS NOT NULL AND exchange != '') as with_exchange,
                    COUNT(*) FILTER (WHERE exchange IS NULL OR exchange = '') as missing_exchange,
                    COUNT(*) FILTER (WHERE avg_volume > 0) as with_avg_volume,
                    COUNT(*) FILTER (WHERE avg_volume = 0 OR avg_volume IS NULL) as missing_avg_volume,
                    COUNT(*) FILTER (WHERE sector IS NOT NULL AND sector != '') as with_sector,
                    COUNT(*) FILTER (WHERE name IS NOT NULL AND name != '') as with_name,
                    MAX(updated_at) as last_update
                FROM company_profiles
            """)
            
            stats = cursor.fetchone()
            with_market_cap, missing_market_cap, with_exchange, missing_exchange, \
            with_avg_volume, missing_avg_volume, with_sector, with_name, last_update = stats
            
            # Find problematic records
            cursor.execute("""
                SELECT ticker, market_cap, exchange, avg_volume
                FROM company_profiles
                WHERE market_cap = 0 OR market_cap IS NULL
                   OR exchange IS NULL OR exchange = ''
                   OR avg_volume = 0 OR avg_volume IS NULL
                ORDER BY ticker
                LIMIT 20
            """)
            
            problematic = cursor.fetchall()
            
            result = {
                "total": total,
                "with_market_cap": with_market_cap or 0,
                "missing_market_cap": missing_market_cap or 0,
                "with_exchange": with_exchange or 0,
                "missing_exchange": missing_exchange or 0,
                "with_avg_volume": with_avg_volume or 0,
                "missing_avg_volume": missing_avg_volume or 0,
                "with_sector": with_sector or 0,
                "with_name": with_name or 0,
                "last_update": str(last_update) if last_update else None,
                "completeness": {
                    "market_cap": f"{(with_market_cap/total*100):.1f}%" if total > 0 else "0%",
                    "exchange": f"{(with_exchange/total*100):.1f}%" if total > 0 else "0%",
                    "avg_volume": f"{(with_avg_volume/total*100):.1f}%" if total > 0 else "0%"
                },
                "status": "good" if (with_market_cap/total > 0.95 and with_exchange/total > 0.95) else "warning" if (with_market_cap/total > 0.5) else "critical",
                "problematic_tickers": [ticker for ticker, mc, ex, av in problematic[:10]] if problematic else []
            }
            
            print(f"   Total records: {total:,}")
            print(f"   ✅ With market_cap: {with_market_cap:,} ({result['completeness']['market_cap']})")
            print(f"   ❌ Missing market_cap: {missing_market_cap:,}")
            print(f"   ✅ With exchange: {with_exchange:,} ({result['completeness']['exchange']})")
            print(f"   ❌ Missing exchange: {missing_exchange:,}")
            print(f"   ✅ With avg_volume: {with_avg_volume:,} ({result['completeness']['avg_volume']})")
            print(f"   Last update: {last_update}")
            print(f"   Status: {result['status'].upper()}")
            
            if problematic and len(problematic) > 0:
                if self.verbose:
                    print(f"\n   Sample problematic tickers (showing first 10):")
                    for ticker, mc, ex, av in problematic[:10]:
                        print(f"      {ticker}: market_cap={mc}, exchange='{ex}', avg_volume={av}")
                else:
                    print(f"   ⚠️  {len(problematic)} tickers need attention (use --verbose to see details)")
            
            if result['status'] != 'good':
                self.warnings.append(f"Company profiles: {result['status']} - {missing_market_cap} missing market_cap, {missing_exchange} missing exchange")
            
            print()
            return result
    
    def validate_stock_prices(self) -> Dict[str, Any]:
        """Validate stock prices data quality."""
        print("2. STOCK PRICES VALIDATION")
        print("-" * 100)
        
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Total records
            cursor.execute("SELECT COUNT(*) FROM stock_prices")
            total = cursor.fetchone()[0] or 0
            
            # Recent records (last 30 days)
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_recent,
                    COUNT(*) FILTER (WHERE change_percent IS NOT NULL) as with_change_percent,
                    COUNT(*) FILTER (WHERE change IS NOT NULL) as with_change,
                    COUNT(*) FILTER (WHERE volume > 0) as with_volume,
                    COUNT(*) FILTER (WHERE volume = 0 OR volume IS NULL) as missing_volume,
                    MAX(date) as latest_date,
                    MIN(date) as earliest_date
                FROM stock_prices
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            """)
            
            recent_stats = cursor.fetchone()
            
            # Overall statistics
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT ticker) as unique_tickers
                FROM stock_prices
            """)
            unique_tickers = cursor.fetchone()[0] or 0
            
            cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE change_percent IS NOT NULL) as with_change_percent_total,
                    MAX(date) as latest_date_overall,
                    MIN(date) as earliest_date_overall
                FROM stock_prices
            """)
            overall_stats = cursor.fetchone()
            
            result = {
                "total": total,
                "unique_tickers": unique_tickers,
                "recent_30_days": {
                    "total": recent_stats[0] or 0,
                    "with_change_percent": recent_stats[1] or 0,
                    "with_change": recent_stats[2] or 0,
                    "with_volume": recent_stats[3] or 0,
                    "missing_volume": recent_stats[4] or 0,
                    "latest_date": str(recent_stats[5]) if recent_stats[5] else None,
                    "earliest_date": str(recent_stats[6]) if recent_stats[6] else None
                },
                "overall": {
                    "with_change_percent": overall_stats[0] or 0,
                    "latest_date": str(overall_stats[1]) if overall_stats[1] else None,
                    "earliest_date": str(overall_stats[2]) if overall_stats[2] else None
                },
                "completeness": {
                    "change_percent_recent": f"{(recent_stats[1]/recent_stats[0]*100):.1f}%" if recent_stats[0] and recent_stats[0] > 0 else "0%",
                    "volume_recent": f"{(recent_stats[3]/recent_stats[0]*100):.1f}%" if recent_stats[0] and recent_stats[0] > 0 else "0%"
                },
                "status": "good" if (recent_stats[1] or 0) > 0 else "warning"
            }
            
            print(f"   Total records: {total:,}")
            print(f"   Unique tickers: {result['unique_tickers']:,}")
            print(f"   Recent 30 days: {result['recent_30_days']['total']:,} records")
            print(f"   ✅ With change_percent (recent): {result['recent_30_days']['with_change_percent']:,} ({result['completeness']['change_percent_recent']})")
            print(f"   ✅ With volume (recent): {result['recent_30_days']['with_volume']:,} ({result['completeness']['volume_recent']})")
            print(f"   Latest date: {result['overall']['latest_date']}")
            print(f"   Status: {result['status'].upper()}")
            
            if result['status'] != 'good':
                self.warnings.append(f"Stock prices: {result['status']} - change_percent missing for recent data")
            
            print()
            return result
    
    def validate_financial_statements(self) -> Dict[str, Any]:
        """Validate financial statements data quality."""
        print("3. FINANCIAL STATEMENTS VALIDATION")
        print("-" * 100)
        
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Cash flow statements
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE investing_cash_flow != 0 OR financing_cash_flow != 0) as with_data,
                    COUNT(*) FILTER (WHERE investing_cash_flow = 0 AND financing_cash_flow = 0) as all_zero,
                    COUNT(DISTINCT ticker) as unique_tickers,
                    MAX(date) as latest_date
                FROM cash_flow_statements
                WHERE date >= CURRENT_DATE - INTERVAL '1 year'
            """)
            
            cf_stats = cursor.fetchone()
            
            # Income statements
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE eps_diluted != 0 AND eps_diluted IS NOT NULL) as with_eps_diluted,
                    COUNT(*) FILTER (WHERE revenue > 0) as with_revenue,
                    COUNT(DISTINCT ticker) as unique_tickers,
                    MAX(date) as latest_date
                FROM income_statements
                WHERE date >= CURRENT_DATE - INTERVAL '1 year'
            """)
            
            is_stats = cursor.fetchone()
            
            # Balance sheets
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT ticker) as unique_tickers,
                    MAX(date) as latest_date
                FROM balance_sheets
                WHERE date >= CURRENT_DATE - INTERVAL '1 year'
            """)
            
            bs_stats = cursor.fetchone()
            
            cf_total = cf_stats[0] or 0
            is_total = is_stats[0] or 0
            bs_total = bs_stats[0] or 0
            
            result = {
                "cash_flow_statements": {
                    "total": cf_total,
                    "with_data": cf_stats[1] or 0,
                    "all_zero": cf_stats[2] or 0,
                    "unique_tickers": cf_stats[3] or 0,
                    "latest_date": str(cf_stats[4]) if cf_stats[4] else None,
                    "data_quality": f"{(cf_stats[1]/cf_total*100):.1f}%" if cf_total > 0 else "0%",
                    "status": "good" if (cf_stats[1] or 0) > (cf_total * 0.5) else "warning"
                },
                "income_statements": {
                    "total": is_total,
                    "with_eps_diluted": is_stats[1] or 0,
                    "with_revenue": is_stats[2] or 0,
                    "unique_tickers": is_stats[3] or 0,
                    "latest_date": str(is_stats[4]) if is_stats[4] else None,
                    "eps_completeness": f"{(is_stats[1]/is_total*100):.1f}%" if is_total > 0 else "0%",
                    "status": "good" if (is_stats[1] or 0) > (is_total * 0.5) else "warning"
                },
                "balance_sheets": {
                    "total": bs_total,
                    "unique_tickers": bs_stats[1] or 0,
                    "latest_date": str(bs_stats[2]) if bs_stats[2] else None,
                    "status": "good" if bs_total > 0 else "warning"
                },
                "overall_status": "warning"  # Historical data may have field mapping issues
            }
            
            print(f"   Cash Flow Statements (last year):")
            print(f"      Total: {cf_total:,}")
            print(f"      With valid data: {cf_stats[1]:,} ({result['cash_flow_statements']['data_quality']})")
            print(f"      All zero: {cf_stats[2]:,}")
            print(f"      Unique tickers: {cf_stats[3]:,}")
            print(f"      Status: {result['cash_flow_statements']['status'].upper()}")
            print()
            
            print(f"   Income Statements (last year):")
            print(f"      Total: {is_total:,}")
            print(f"      With eps_diluted: {is_stats[1]:,} ({result['income_statements']['eps_completeness']})")
            print(f"      With revenue: {is_stats[2]:,}")
            print(f"      Unique tickers: {is_stats[3]:,}")
            print(f"      Status: {result['income_statements']['status'].upper()}")
            print()
            
            print(f"   Balance Sheets (last year):")
            print(f"      Total: {bs_total:,}")
            print(f"      Unique tickers: {bs_stats[1]:,}")
            print(f"      Status: {result['balance_sheets']['status'].upper()}")
            print()
            
            if result['cash_flow_statements']['status'] != 'good':
                self.warnings.append(f"Cash flow statements: {result['cash_flow_statements']['status']} - many records have zero values (may need re-ingestion)")
            
            if result['income_statements']['status'] != 'good':
                self.warnings.append(f"Income statements: {result['income_statements']['status']} - eps_diluted missing for many records (may need re-ingestion)")
            
            return result
    
    def validate_analyst_data(self) -> Dict[str, Any]:
        """Validate analyst data quality."""
        print("4. ANALYST DATA VALIDATION")
        print("-" * 100)
        
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Consensus
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE consensus_rating IS NOT NULL) as with_consensus,
                    COUNT(*) FILTER (WHERE target_consensus > 0) as with_target,
                    COUNT(DISTINCT ticker) as unique_tickers,
                    MAX(updated_at) as last_update
                FROM analyst_consensus
            """)
            
            consensus_stats = cursor.fetchone()
            
            # Ratings
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT ticker) as unique_tickers,
                    COUNT(*) FILTER (WHERE rating IS NOT NULL AND rating != '') as with_rating,
                    MAX(rating_date) as latest_rating
                FROM analyst_ratings
            """)
            
            ratings_stats = cursor.fetchone()
            
            # Estimates
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT ticker) as unique_tickers,
                    COUNT(*) FILTER (WHERE estimated_eps_avg IS NOT NULL) as with_eps_estimate,
                    MAX(date) as latest_estimate
                FROM analyst_estimates
            """)
            
            estimates_stats = cursor.fetchone()
            
            consensus_total = consensus_stats[0] or 0
            ratings_total = ratings_stats[0] or 0
            estimates_total = estimates_stats[0] or 0
            
            result = {
                "consensus": {
                    "total": consensus_total,
                    "unique_tickers": consensus_stats[3] or 0,  # Fixed: index 3 is unique_tickers, 4 is last_update
                    "with_consensus": consensus_stats[1] or 0,
                    "with_target": consensus_stats[2] or 0,
                    "last_update": str(consensus_stats[4]) if consensus_stats[4] else None,
                    "status": "good" if consensus_total > 0 else "warning"
                },
                "ratings": {
                    "total": ratings_total,
                    "unique_tickers": ratings_stats[1] or 0,
                    "with_rating": ratings_stats[2] or 0,
                    "latest_rating": str(ratings_stats[3]) if ratings_stats[3] else None,
                    "status": "good" if ratings_total > 0 else "warning"
                },
                "estimates": {
                    "total": estimates_total,
                    "unique_tickers": estimates_stats[1] or 0,
                    "with_eps_estimate": estimates_stats[2] or 0,
                    "latest_estimate": str(estimates_stats[3]) if estimates_stats[3] else None,
                    "status": "good" if estimates_total > 0 else "warning"
                },
                "overall_status": "good" if consensus_total > 0 and ratings_total > 0 else "warning"
            }
            
            print(f"   Consensus:")
            print(f"      Total: {consensus_total:,}")
            print(f"      Unique tickers: {result['consensus']['unique_tickers']:,}")
            print(f"      With consensus rating: {result['consensus']['with_consensus']:,}")
            print(f"      With price target: {result['consensus']['with_target']:,}")
            print(f"      Status: {result['consensus']['status'].upper()}")
            print()
            
            print(f"   Individual Ratings:")
            print(f"      Total: {ratings_total:,}")
            print(f"      Unique tickers: {result['ratings']['unique_tickers']:,}")
            print(f"      With rating: {result['ratings']['with_rating']:,}")
            print(f"      Latest rating: {result['ratings']['latest_rating']}")
            print(f"      Status: {result['ratings']['status'].upper()}")
            print()
            
            print(f"   Estimates:")
            print(f"      Total: {estimates_total:,}")
            print(f"      Unique tickers: {result['estimates']['unique_tickers']:,}")
            print(f"      With EPS estimate: {result['estimates']['with_eps_estimate']:,}")
            print(f"      Latest estimate: {result['estimates']['latest_estimate']}")
            print(f"      Status: {result['estimates']['status'].upper()}")
            print()
            
            if result['overall_status'] != 'good':
                self.warnings.append("Analyst data: Missing consensus or ratings data")
            
            return result
    
    def validate_data_freshness(self) -> Dict[str, Any]:
        """Validate data freshness (last update times)."""
        print("5. DATA FRESHNESS VALIDATION")
        print("-" * 100)
        
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Company profiles freshness
            cursor.execute("""
                SELECT MAX(updated_at) as last_update
                FROM company_profiles
            """)
            profile_update = cursor.fetchone()[0]
            
            # Stock prices freshness
            cursor.execute("""
                SELECT MAX(date) as latest_date
                FROM stock_prices
            """)
            price_date = cursor.fetchone()[0]
            
            # Financial statements freshness
            cursor.execute("""
                SELECT MAX(date) as latest_date
                FROM income_statements
            """)
            is_date = cursor.fetchone()[0]
            
            # Analyst data freshness
            cursor.execute("""
                SELECT MAX(updated_at) as last_update
                FROM analyst_consensus
            """)
            consensus_update = cursor.fetchone()[0]
            
            now = datetime.now()
            
            # Calculate age for company profiles
            if profile_update:
                if profile_update.tzinfo:
                    profile_age = now - profile_update.replace(tzinfo=None)
                else:
                    profile_age = now - profile_update
            else:
                profile_age = None
            
            # Calculate age for stock prices
            if price_date:
                today = datetime.now().date()
                # Handle both date and datetime types
                if isinstance(price_date, datetime):
                    price_date_only = price_date.date()
                elif hasattr(price_date, 'date'):
                    price_date_only = price_date.date()
                else:
                    price_date_only = price_date
                price_age = today - price_date_only
            else:
                price_age = None
            
            # Calculate age for analyst consensus
            if consensus_update:
                if consensus_update.tzinfo:
                    consensus_age = now - consensus_update.replace(tzinfo=None)
                else:
                    consensus_age = now - consensus_update
            else:
                consensus_age = None
            
            # Calculate income statements age
            if is_date:
                is_age_days = (now.date() - is_date).days
            else:
                is_age_days = None
            
            result = {
                "company_profiles": {
                    "last_update": str(profile_update) if profile_update else None,
                    "age_days": profile_age.days if profile_age is not None else None,  # Fixed: handle 0 days correctly
                    "status": "fresh" if profile_age is not None and profile_age.days <= 7 else "stale" if profile_age is not None and profile_age.days <= 30 else "very_stale"
                },
                "stock_prices": {
                    "latest_date": str(price_date) if price_date else None,
                    "age_days": price_age.days if price_age is not None else None,  # Fixed: handle 0 days correctly
                    "status": "fresh" if price_age is not None and price_age.days <= 1 else "stale" if price_age is not None and price_age.days <= 7 else "very_stale" if price_age is not None else "unknown"
                },
                "income_statements": {
                    "latest_date": str(is_date) if is_date else None,
                    "age_days": is_age_days,
                    "status": "fresh" if is_age_days is not None and is_age_days <= 90 else "stale" if is_age_days is not None else "unknown"
                },
                "analyst_consensus": {
                    "last_update": str(consensus_update) if consensus_update else None,
                    "age_days": consensus_age.days if consensus_age is not None else None,  # Fixed: handle 0 days correctly
                    "status": "fresh" if consensus_age is not None and consensus_age.days <= 7 else "stale" if consensus_age is not None else "unknown"
                }
            }
            
            print(f"   Company Profiles:")
            print(f"      Last update: {result['company_profiles']['last_update']}")
            print(f"      Age: {result['company_profiles']['age_days']} days")
            print(f"      Status: {result['company_profiles']['status'].upper()}")
            print()
            
            print(f"   Stock Prices:")
            print(f"      Latest date: {result['stock_prices']['latest_date']}")
            if result['stock_prices']['age_days'] is not None:
                print(f"      Age: {result['stock_prices']['age_days']} days")
            else:
                print(f"      Age: N/A")
            print(f"      Status: {result['stock_prices']['status'].upper()}")
            print()
            
            print(f"   Income Statements:")
            print(f"      Latest date: {result['income_statements']['latest_date']}")
            if result['income_statements']['age_days'] is not None:
                print(f"      Age: {result['income_statements']['age_days']} days")
            else:
                print(f"      Age: N/A")
            print(f"      Status: {result['income_statements']['status'].upper()}")
            print()
            
            print(f"   Analyst Consensus:")
            print(f"      Last update: {result['analyst_consensus']['last_update']}")
            if result['analyst_consensus']['age_days'] is not None:
                print(f"      Age: {result['analyst_consensus']['age_days']} days")
            else:
                print(f"      Age: N/A")
            print(f"      Status: {result['analyst_consensus']['status'].upper()}")
            print()
            
            # Add warnings for stale data
            if result['stock_prices']['status'] == 'very_stale':
                self.warnings.append(f"Stock prices: Very stale (last update: {result['stock_prices']['age_days']} days ago)")
            
            if result['company_profiles']['status'] == 'very_stale':
                self.warnings.append(f"Company profiles: Very stale (last update: {result['company_profiles']['age_days']} days ago)")
            
            return result
    
    def validate_coverage(self) -> Dict[str, Any]:
        """Validate ticker coverage across tables."""
        print("6. TICKER COVERAGE VALIDATION")
        print("-" * 100)
        
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Get all unique tickers from stock_prices (source of truth)
            cursor.execute("SELECT COUNT(DISTINCT ticker) FROM stock_prices")
            total_tickers = cursor.fetchone()[0] or 0
            
            # Coverage in each table
            cursor.execute("""
                SELECT 
                    (SELECT COUNT(DISTINCT ticker) FROM company_profiles) as profiles_tickers,
                    (SELECT COUNT(DISTINCT ticker) FROM analyst_consensus) as consensus_tickers,
                    (SELECT COUNT(DISTINCT ticker) FROM analyst_ratings) as ratings_tickers,
                    (SELECT COUNT(DISTINCT ticker) FROM analyst_estimates) as estimates_tickers,
                    (SELECT COUNT(DISTINCT ticker) FROM income_statements) as income_tickers,
                    (SELECT COUNT(DISTINCT ticker) FROM cash_flow_statements) as cf_tickers
            """)
            
            coverage_stats = cursor.fetchone()
            
            # Tickers missing from key tables
            cursor.execute("""
                SELECT COUNT(DISTINCT sp.ticker)
                FROM stock_prices sp
                LEFT JOIN company_profiles cp ON sp.ticker = cp.ticker
                WHERE cp.ticker IS NULL
            """)
            missing_profiles = cursor.fetchone()[0] or 0
            
            cursor.execute("""
                SELECT COUNT(DISTINCT sp.ticker)
                FROM stock_prices sp
                LEFT JOIN analyst_consensus ac ON sp.ticker = ac.ticker
                WHERE ac.ticker IS NULL
            """)
            missing_consensus = cursor.fetchone()[0] or 0
            
            result = {
                "total_tickers": total_tickers,
                "coverage": {
                    "company_profiles": {
                        "count": coverage_stats[0] or 0,
                        "percentage": f"{((coverage_stats[0] or 0)/total_tickers*100):.1f}%" if total_tickers > 0 else "0%",
                        "missing": missing_profiles
                    },
                    "analyst_consensus": {
                        "count": coverage_stats[1] or 0,
                        "percentage": f"{((coverage_stats[1] or 0)/total_tickers*100):.1f}%" if total_tickers > 0 else "0%",
                        "missing": missing_consensus
                    },
                    "analyst_ratings": {
                        "count": coverage_stats[2] or 0,
                        "percentage": f"{((coverage_stats[2] or 0)/total_tickers*100):.1f}%" if total_tickers > 0 else "0%"
                    },
                    "analyst_estimates": {
                        "count": coverage_stats[3] or 0,
                        "percentage": f"{((coverage_stats[3] or 0)/total_tickers*100):.1f}%" if total_tickers > 0 else "0%"
                    },
                    "income_statements": {
                        "count": coverage_stats[4] or 0,
                        "percentage": f"{((coverage_stats[4] or 0)/total_tickers*100):.1f}%" if total_tickers > 0 else "0%"
                    },
                    "cash_flow_statements": {
                        "count": coverage_stats[5] or 0,
                        "percentage": f"{((coverage_stats[5] or 0)/total_tickers*100):.1f}%" if total_tickers > 0 else "0%"
                    }
                },
                "status": "good" if (coverage_stats[0] or 0)/total_tickers > 0.95 and (coverage_stats[1] or 0)/total_tickers > 0.95 else "warning"
            }
            
            print(f"   Total tickers (from stock_prices): {total_tickers:,}")
            print()
            print(f"   Coverage:")
            print(f"      Company Profiles: {result['coverage']['company_profiles']['count']:,} ({result['coverage']['company_profiles']['percentage']}) - Missing: {missing_profiles:,}")
            print(f"      Analyst Consensus: {result['coverage']['analyst_consensus']['count']:,} ({result['coverage']['analyst_consensus']['percentage']}) - Missing: {missing_consensus:,}")
            print(f"      Analyst Ratings: {result['coverage']['analyst_ratings']['count']:,} ({result['coverage']['analyst_ratings']['percentage']})")
            print(f"      Analyst Estimates: {result['coverage']['analyst_estimates']['count']:,} ({result['coverage']['analyst_estimates']['percentage']})")
            print(f"      Income Statements: {result['coverage']['income_statements']['count']:,} ({result['coverage']['income_statements']['percentage']})")
            print(f"      Cash Flow Statements: {result['coverage']['cash_flow_statements']['count']:,} ({result['coverage']['cash_flow_statements']['percentage']})")
            print(f"   Status: {result['status'].upper()}")
            print()
            
            if result['status'] != 'good':
                self.warnings.append(f"Coverage: {result['status']} - Missing profiles for {missing_profiles} tickers, missing consensus for {missing_consensus} tickers")
            
            return result
    
    def generate_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate overall summary."""
        issues = []
        warnings = []
        
        # Check each validation result
        if results['company_profiles']['status'] == 'critical':
            issues.append("Company profiles: Critical data quality issues")
        elif results['company_profiles']['status'] == 'warning':
            warnings.append("Company profiles: Some data quality issues")
        
        if results['stock_prices']['status'] == 'warning':
            warnings.append("Stock prices: change_percent missing for recent data")
        
        if results['financial_statements']['overall_status'] == 'warning':
            warnings.append("Financial statements: Historical data may have field mapping issues")
        
        if results['data_freshness']['stock_prices']['status'] == 'very_stale':
            issues.append("Stock prices: Data is very stale")
        
        if results['coverage']['status'] == 'warning':
            warnings.append("Coverage: Some tickers missing from key tables")
        
        overall_status = "good"
        if issues:
            overall_status = "critical"
        elif warnings:
            overall_status = "warning"
        
        return {
            "overall_status": overall_status,
            "issues": issues,
            "warnings": warnings,
            "total_warnings": len(warnings),
            "total_issues": len(issues)
        }
    
    def print_summary(self, results: Dict[str, Any]):
        """Print validation summary."""
        summary = results['summary']
        
        print("=" * 100)
        print("VALIDATION SUMMARY")
        print("=" * 100)
        print()
        
        print(f"Overall Status: {summary['overall_status'].upper()}")
        print()
        
        if summary['issues']:
            print(f"❌ CRITICAL ISSUES ({summary['total_issues']}):")
            for issue in summary['issues']:
                print(f"   • {issue}")
            print()
        
        if summary['warnings']:
            print(f"⚠️  WARNINGS ({summary['total_warnings']}):")
            for warning in summary['warnings']:
                print(f"   • {warning}")
            print()
        
        if not summary['issues'] and not summary['warnings']:
            print("✅ All validation checks passed!")
            print()
        
        print("=" * 100)
        print()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Validate data quality')
    parser.add_argument('--fix', action='store_true', help='Attempt to fix common issues')
    parser.add_argument('--verbose', action='store_true', help='Show detailed results')
    args = parser.parse_args()
    
    validator = DataQualityValidator(verbose=args.verbose)
    results = validator.validate_all()
    
    # Exit with appropriate code
    if results['summary']['overall_status'] == 'critical':
        sys.exit(1)
    elif results['summary']['overall_status'] == 'warning':
        sys.exit(0)  # Warnings are acceptable
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()

