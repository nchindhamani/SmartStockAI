#!/usr/bin/env python3
"""
Interactive Database Query Tool for SmartStock AI

This script provides example queries for all data types in the database.
You can modify the queries or add your own.

Usage:
    python query_database.py
    # Or run specific queries by modifying the script
"""

from data.db_connection import get_connection
from tabulate import tabulate
import sys


def print_table(title: str, headers: list, rows: list):
    """Print data in a formatted table."""
    print("\n" + "=" * 100)
    print(f"📊 {title}")
    print("=" * 100)
    if rows:
        print(tabulate(rows, headers=headers, tablefmt="grid", floatfmt=".2f"))
    else:
        print("No data found.")
    print()


def query_stock_prices(ticker: str = "AAPL", limit: int = 10):
    """Query recent stock prices for a ticker."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                date,
                open,
                high,
                low,
                close,
                volume,
                change,
                change_percent,
                vwap
            FROM stock_prices
            WHERE ticker = %s
            ORDER BY date DESC
            LIMIT %s
        """, (ticker, limit))
        
        rows = cursor.fetchall()
        headers = ["Date", "Open", "High", "Low", "Close", "Volume", "Change", "Change %", "VWAP"]
        print_table(f"Stock Prices - {ticker} (Last {limit} days)", headers, rows)


def query_company_profiles(ticker: str = "AAPL"):
    """Query company profile data."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                ticker,
                name,
                sector,
                industry,
                market_cap,
                exchange,
                website,
                description
            FROM company_profiles
            WHERE ticker = %s
        """, (ticker,))
        
        rows = cursor.fetchall()
        headers = ["Ticker", "Company Name", "Sector", "Industry", "Market Cap", "Exchange", "Website", "Description"]
        print_table(f"Company Profile - {ticker}", headers, rows)


def query_key_metrics(ticker: str = "AAPL", limit: int = 10):
    """Query key financial metrics."""
    with get_connection() as conn:
        cursor = conn.cursor()
        # Financial metrics are stored as key-value pairs, so we need to pivot
        # Query the most important metrics
        key_metric_names = [
            'pe_ratio', 'price_to_sales_ratio', 'pb_ratio', 'debt_to_equity',
            'roe', 'roic', 'current_ratio', 'dividend_yield', 'market_cap'
        ]
        
        # Get unique dates for this ticker
        cursor.execute("""
            SELECT DISTINCT period_end_date
            FROM financial_metrics
            WHERE ticker = %s
            AND period_end_date IS NOT NULL
            ORDER BY period_end_date DESC
            LIMIT %s
        """, (ticker, limit))
        
        dates = [row[0] for row in cursor.fetchall()]
        
        if not dates:
            print_table(f"Key Financial Metrics - {ticker} (Last {limit} years)", ["No data found"], [])
            return
        
        # Build a query to pivot the data
        rows = []
        for date in dates:
            row_data = [date]
            for metric_name in key_metric_names:
                cursor.execute("""
                    SELECT metric_value
                    FROM financial_metrics
                    WHERE ticker = %s
                    AND metric_name = %s
                    AND period_end_date = %s
                    AND period = 'FY'
                    LIMIT 1
                """, (ticker, metric_name, date))
                result = cursor.fetchone()
                row_data.append(result[0] if result and result[0] is not None else None)
            rows.append(row_data)
        
        headers = ["Date"] + [name.replace('_', ' ').title() for name in key_metric_names]
        print_table(f"Key Financial Metrics - {ticker} (Last {limit} years)", headers, rows)


def query_analyst_ratings(ticker: str = "AAPL", limit: int = 10):
    """Query individual analyst ratings."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                rating_date,
                analyst,
                previous_rating,
                rating,
                action,
                price_target
            FROM analyst_ratings
            WHERE ticker = %s
            ORDER BY rating_date DESC
            LIMIT %s
        """, (ticker, limit))
        
        rows = cursor.fetchall()
        headers = ["Date", "Analyst", "Previous Rating", "Rating", "Action", "Price Target"]
        print_table(f"Analyst Ratings - {ticker} (Last {limit})", headers, rows)


def query_analyst_estimates(ticker: str = "AAPL", limit: int = 10):
    """Query analyst estimates."""
    with get_connection() as conn:
        cursor = conn.cursor()
        # Check actual column names
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'analyst_estimates'
            ORDER BY ordinal_position
        """)
        columns = [row[0] for row in cursor.fetchall()]
        
        # Build query based on actual columns
        if 'period' in columns:
            period_col = 'period'
        else:
            period_col = None
        
        if 'revenue_avg' in columns:
            rev_avg = 'revenue_avg'
        elif 'estimated_revenue_avg' in columns:
            rev_avg = 'estimated_revenue_avg'
        else:
            rev_avg = None
        
        if period_col:
            query = f"""
                SELECT 
                    date,
                    {period_col} as period,
                    estimated_revenue_low,
                    estimated_revenue_high,
                    estimated_revenue_avg,
                    estimated_eps_low,
                    estimated_eps_high,
                    estimated_eps_avg,
                    number_of_analysts_revenue,
                    number_of_analysts_eps
                FROM analyst_estimates
                WHERE ticker = %s
                ORDER BY date DESC
                LIMIT %s
            """
            headers = ["Date", "Period", "Rev Low", "Rev High", "Rev Avg", 
                      "EPS Low", "EPS High", "EPS Avg", "Analysts (Rev)", "Analysts (EPS)"]
        else:
            query = f"""
                SELECT 
                    date,
                    estimated_revenue_low,
                    estimated_revenue_high,
                    estimated_revenue_avg,
                    estimated_eps_low,
                    estimated_eps_high,
                    estimated_eps_avg,
                    number_of_analysts_revenue,
                    number_of_analysts_eps
                FROM analyst_estimates
                WHERE ticker = %s
                ORDER BY date DESC
                LIMIT %s
            """
            headers = ["Date", "Rev Low", "Rev High", "Rev Avg", 
                      "EPS Low", "EPS High", "EPS Avg", "Analysts (Rev)", "Analysts (EPS)"]
        
        cursor.execute(query, (ticker, limit))
        rows = cursor.fetchall()
        print_table(f"Analyst Estimates - {ticker} (Last {limit})", headers, rows)


def query_analyst_consensus(ticker: str = "AAPL"):
    """Query analyst consensus data."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                ticker,
                strong_buy,
                buy,
                hold,
                sell,
                strong_sell,
                consensus_rating,
                target_high,
                target_low,
                target_consensus,
                target_median,
                last_month_count,
                last_month_avg_price_target,
                last_quarter_count,
                last_quarter_avg_price_target,
                updated_at
            FROM analyst_consensus
            WHERE ticker = %s
            ORDER BY updated_at DESC
            LIMIT 1
        """, (ticker,))
        
        rows = cursor.fetchall()
        headers = ["Ticker", "Strong Buy", "Buy", "Hold", "Sell", "Strong Sell", "Consensus", 
                  "Target High", "Target Low", "Target Consensus", "Target Median",
                  "Last Month Count", "Last Month Avg Target", "Last Q Count", "Last Q Avg Target", "Updated"]
        print_table(f"Analyst Consensus - {ticker}", headers, rows)


def query_income_statements(ticker: str = "AAPL", limit: int = 5):
    """Query income statements."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                date,
                period,
                revenue,
                cost_of_revenue,
                gross_profit,
                operating_expenses,
                operating_income,
                interest_expense,
                income_tax_expense,
                net_income,
                eps,
                eps_diluted
            FROM income_statements
            WHERE ticker = %s
            ORDER BY date DESC
            LIMIT %s
        """, (ticker, limit))
        
        rows = cursor.fetchall()
        headers = ["Date", "Period", "Revenue", "Cost of Revenue", "Gross Profit", "Operating Expenses", 
                  "Operating Income", "Interest Expense", "Income Tax", "Net Income", "EPS", "EPS Diluted"]
        print_table(f"Income Statements - {ticker} (Last {limit})", headers, rows)


def query_balance_sheets(ticker: str = "AAPL", limit: int = 5):
    """Query balance sheets."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                date,
                period,
                cash_and_equivalents,
                short_term_investments,
                accounts_receivable,
                inventory,
                total_assets,
                accounts_payable,
                short_term_debt,
                long_term_debt,
                total_liabilities,
                total_equity,
                retained_earnings
            FROM balance_sheets
            WHERE ticker = %s
            ORDER BY date DESC
            LIMIT %s
        """, (ticker, limit))
        
        rows = cursor.fetchall()
        headers = ["Date", "Period", "Cash", "ST Investments", "Receivables", "Inventory", "Total Assets",
                  "AP", "ST Debt", "LT Debt", "Total Liab", "Equity", "Retained Earnings"]
        print_table(f"Balance Sheets - {ticker} (Last {limit})", headers, rows)


def query_cash_flow_statements(ticker: str = "AAPL", limit: int = 5):
    """Query cash flow statements."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                date,
                period,
                operating_cash_flow,
                investing_cash_flow,
                financing_cash_flow,
                free_cash_flow,
                capital_expenditure,
                dividends_paid,
                stock_repurchased,
                debt_repayment
            FROM cash_flow_statements
            WHERE ticker = %s
            ORDER BY date DESC
            LIMIT %s
        """, (ticker, limit))
        
        rows = cursor.fetchall()
        headers = ["Date", "Period", "Operating CF", "Investing CF", "Financing CF", "FCF",
                  "CapEx", "Dividends", "Stock Repurchased", "Debt Repayment"]
        print_table(f"Cash Flow Statements - {ticker} (Last {limit})", headers, rows)


def query_earnings_surprises(ticker: str = "AAPL", limit: int = 10):
    """Query earnings surprises (actual vs estimated) for a ticker."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                date,
                eps_actual,
                eps_estimated,
                surprise_percent,
                revenue_actual,
                revenue_estimated,
                source
            FROM earnings_data
            WHERE ticker = %s
            ORDER BY date DESC
            LIMIT %s
        """, (ticker, limit))
        
        rows = cursor.fetchall()
        
        # Format the rows for display
        formatted_rows = []
        for row in rows:
            date_val = row[0]
            eps_actual = f"${row[1]:.2f}" if row[1] else "N/A"
            eps_estimated = f"${row[2]:.2f}" if row[2] else "N/A"
            
            # Format surprise with direction
            if row[3] is not None:
                surprise = row[3]
                direction = "BEAT ✅" if surprise > 0 else "MISS ❌" if surprise < 0 else "MATCH ⚪"
                surprise_str = f"{surprise:.2f}% ({direction})"
            else:
                surprise_str = "N/A"
            
            revenue_actual_str = f"${row[4]/1e9:.2f}B" if row[4] else "N/A"
            revenue_est_str = f"${row[5]/1e9:.2f}B" if row[5] else "N/A"
            source = row[6] or "N/A"
            
            formatted_rows.append([
                date_val,
                eps_actual,
                eps_estimated,
                surprise_str,
                revenue_actual_str,
                revenue_est_str,
                source
            ])
        
        headers = ["Date", "EPS Actual", "EPS Estimated", "Surprise %", 
                  "Revenue Actual (B)", "Revenue Est (B)", "Source"]
        print_table(f"Earnings Surprises - {ticker} (Last {limit})", headers, formatted_rows)


def get_database_stats():
    """Get overall database statistics."""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        stats = []
        
        # Count records in each table
        tables = [
            ("stock_prices", "Stock Prices"),
            ("company_profiles", "Company Profiles"),
            ("financial_metrics", "Financial Metrics"),
            ("income_statements", "Income Statements"),
            ("balance_sheets", "Balance Sheets"),
            ("cash_flow_statements", "Cash Flow Statements"),
            ("analyst_ratings", "Analyst Ratings"),
            ("analyst_estimates", "Analyst Estimates"),
            ("analyst_consensus", "Analyst Consensus"),
            ("earnings_data", "Earnings Surprises"),
        ]
        
        for table, name in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            stats.append((name, f"{count:,}"))
        
        headers = ["Table", "Record Count"]
        print_table("Database Statistics", headers, stats)


def main():
    """Main function to run example queries."""
    if len(sys.argv) > 1:
        ticker = sys.argv[1].upper()
    else:
        ticker = "AAPL"
    
    print("\n" + "=" * 100)
    print("🔍 SmartStock AI - Database Query Tool")
    print("=" * 100)
    print(f"\nQuerying data for: {ticker}")
    print("\nTip: Run with a ticker argument: python query_database.py TSLA")
    
    # Show database stats
    get_database_stats()
    
    # Query different data types
    query_stock_prices(ticker)
    query_company_profiles(ticker)
    query_key_metrics(ticker, limit=5)
    query_analyst_ratings(ticker, limit=10)
    query_analyst_estimates(ticker, limit=5)
    query_analyst_consensus(ticker)
    query_income_statements(ticker, limit=3)
    query_balance_sheets(ticker, limit=3)
    query_cash_flow_statements(ticker, limit=3)
    query_earnings_surprises(ticker, limit=5)
    
    print("\n" + "=" * 100)
    print("✅ Query Complete!")
    print("=" * 100)
    print("\n💡 To modify queries, edit query_database.py")
    print("💡 To run custom SQL, use psql or create a new function in this script")
    print()


if __name__ == "__main__":
    main()

