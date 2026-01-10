# data/financial_statements_store.py
# PostgreSQL store for FMP Premium Financial Statements
# Stores income statements, balance sheets, cash flow statements

import json
from typing import List, Optional, Dict, Any
from datetime import datetime
from data.db_connection import get_connection


class FinancialStatementsStore:
    """
    PostgreSQL-based store for financial statements from FMP.
    
    Stores:
    - Income statements (quarterly and annual)
    - Balance sheets (quarterly and annual)
    - Cash flow statements (quarterly and annual)
    - Earnings data (historical surprises)
    - Insider trading
    - Institutional holdings
    """
    
    def __init__(self):
        """Initialize the financial statements store."""
        self._init_tables()
    
    def _init_tables(self):
        """Initialize the database schema."""
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Income statements table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS income_statements (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    period VARCHAR(10) NOT NULL,
                    revenue DOUBLE PRECISION,
                    gross_profit DOUBLE PRECISION,
                    operating_income DOUBLE PRECISION,
                    net_income DOUBLE PRECISION,
                    eps DOUBLE PRECISION,
                    eps_diluted DOUBLE PRECISION,
                    cost_of_revenue DOUBLE PRECISION,
                    operating_expenses DOUBLE PRECISION,
                    interest_expense DOUBLE PRECISION,
                    income_tax_expense DOUBLE PRECISION,
                    ebitda DOUBLE PRECISION,
                    source VARCHAR(50) DEFAULT 'FMP',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, date, period)
                )
            """)
            
            # Balance sheets table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS balance_sheets (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    period VARCHAR(10) NOT NULL,
                    total_assets DOUBLE PRECISION,
                    total_liabilities DOUBLE PRECISION,
                    total_equity DOUBLE PRECISION,
                    cash_and_equivalents DOUBLE PRECISION,
                    short_term_investments DOUBLE PRECISION,
                    total_debt DOUBLE PRECISION,
                    long_term_debt DOUBLE PRECISION,
                    short_term_debt DOUBLE PRECISION,
                    inventory DOUBLE PRECISION,
                    accounts_receivable DOUBLE PRECISION,
                    accounts_payable DOUBLE PRECISION,
                    retained_earnings DOUBLE PRECISION,
                    source VARCHAR(50) DEFAULT 'FMP',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, date, period)
                )
            """)
            
            # Cash flow statements table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cash_flow_statements (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    period VARCHAR(10) NOT NULL,
                    operating_cash_flow DOUBLE PRECISION,
                    investing_cash_flow DOUBLE PRECISION,
                    financing_cash_flow DOUBLE PRECISION,
                    free_cash_flow DOUBLE PRECISION,
                    capital_expenditure DOUBLE PRECISION,
                    dividends_paid DOUBLE PRECISION,
                    stock_repurchased DOUBLE PRECISION,
                    debt_repayment DOUBLE PRECISION,
                    source VARCHAR(50) DEFAULT 'FMP',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, date, period)
                )
            """)
            
            # Earnings data table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS earnings_data (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    eps_actual DOUBLE PRECISION,
                    eps_estimated DOUBLE PRECISION,
                    revenue_actual DOUBLE PRECISION,
                    revenue_estimated DOUBLE PRECISION,
                    surprise_percent DOUBLE PRECISION,
                    source VARCHAR(50) DEFAULT 'FMP',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, date)
                )
            """)
            
            # Insider trading table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS insider_trades (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    filing_date DATE NOT NULL,
                    transaction_date DATE,
                    insider_name VARCHAR(255),
                    insider_title VARCHAR(255),
                    transaction_type VARCHAR(50),
                    shares BIGINT,
                    price DOUBLE PRECISION,
                    value DOUBLE PRECISION,
                    source VARCHAR(50) DEFAULT 'FMP',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, filing_date, insider_name, transaction_type, shares)
                )
            """)
            
            # Institutional holdings table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS institutional_holdings (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    holder_name VARCHAR(255) NOT NULL,
                    shares BIGINT,
                    value DOUBLE PRECISION,
                    weight_percent DOUBLE PRECISION,
                    change_shares BIGINT,
                    change_percent DOUBLE PRECISION,
                    filing_date DATE,
                    source VARCHAR(50) DEFAULT 'FMP',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, holder_name, filing_date)
                )
            """)
            
            # Company profiles table (detailed from FMP)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS company_profiles (
                    ticker VARCHAR(10) PRIMARY KEY,
                    name TEXT,
                    exchange VARCHAR(50),
                    sector VARCHAR(100),
                    industry VARCHAR(200),
                    description TEXT,
                    ceo VARCHAR(200),
                    website VARCHAR(500),
                    country VARCHAR(100),
                    city VARCHAR(100),
                    employees INTEGER,
                    market_cap DOUBLE PRECISION,
                    beta DOUBLE PRECISION,
                    price DOUBLE PRECISION,
                    avg_volume BIGINT,
                    ipo_date DATE,
                    is_actively_trading BOOLEAN DEFAULT TRUE,
                    source VARCHAR(50) DEFAULT 'FMP',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Index membership table (many-to-many: ticker can be in multiple indices)
            # Note: No foreign key to company_profiles to allow index membership for tickers without profiles
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS index_membership (
                    ticker VARCHAR(10) NOT NULL,
                    index_name VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (ticker, index_name)
                )
            """)
            
            # Create index for fast lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_index_membership_index_name 
                ON index_membership(index_name)
            """)
            
            # ESG scores table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS esg_scores (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    date DATE,
                    esg_score DOUBLE PRECISION,
                    environmental_score DOUBLE PRECISION,
                    social_score DOUBLE PRECISION,
                    governance_score DOUBLE PRECISION,
                    esg_risk_rating VARCHAR(50),
                    source VARCHAR(50) DEFAULT 'FMP',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, date)
                )
            """)
            
            # DCF valuations table (latest only - one record per ticker)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dcf_valuations (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    date DATE,
                    dcf_value DOUBLE PRECISION,
                    stock_price DOUBLE PRECISION,
                    upside_percent DOUBLE PRECISION,
                    source VARCHAR(50) DEFAULT 'FMP',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Add unique constraint on ticker (if table already exists, this will be handled by cleanup script)
            cursor.execute("""
                DO $$ 
                BEGIN
                    -- Drop old (ticker, date) constraint if it exists
                    IF EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'dcf_valuations_ticker_date_key'
                    ) THEN
                        ALTER TABLE dcf_valuations 
                        DROP CONSTRAINT dcf_valuations_ticker_date_key;
                    END IF;
                    
                    -- Add new ticker-only unique constraint if it doesn't exist
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'dcf_valuations_ticker_key'
                    ) THEN
                        ALTER TABLE dcf_valuations 
                        ADD CONSTRAINT dcf_valuations_ticker_key UNIQUE (ticker);
                    END IF;
                    
                    -- Add updated_at column if it doesn't exist
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'dcf_valuations' AND column_name = 'updated_at'
                    ) THEN
                        ALTER TABLE dcf_valuations 
                        ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                    END IF;
                END $$;
            """)
            
            # Analyst estimates table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analyst_estimates (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    estimated_revenue_avg DOUBLE PRECISION,
                    estimated_revenue_low DOUBLE PRECISION,
                    estimated_revenue_high DOUBLE PRECISION,
                    estimated_eps_avg DOUBLE PRECISION,
                    estimated_eps_low DOUBLE PRECISION,
                    estimated_eps_high DOUBLE PRECISION,
                    estimated_ebit_avg DOUBLE PRECISION,
                    estimated_net_income_avg DOUBLE PRECISION,
                    forecast_dispersion DOUBLE PRECISION,
                    actual_eps DOUBLE PRECISION,
                    number_of_analysts_revenue INTEGER,
                    number_of_analysts_eps INTEGER,
                    source VARCHAR(50) DEFAULT 'FMP',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, date)
                )
            """)
            
            # Analyst consensus table (grades, price targets, summary)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analyst_consensus (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    -- Grades consensus
                    strong_buy INTEGER DEFAULT 0,
                    buy INTEGER DEFAULT 0,
                    hold INTEGER DEFAULT 0,
                    sell INTEGER DEFAULT 0,
                    strong_sell INTEGER DEFAULT 0,
                    consensus_rating VARCHAR(50),
                    -- Price target consensus
                    target_high DOUBLE PRECISION,
                    target_low DOUBLE PRECISION,
                    target_consensus DOUBLE PRECISION,
                    target_median DOUBLE PRECISION,
                    -- Price target summary
                    last_month_count INTEGER,
                    last_month_avg_price_target DOUBLE PRECISION,
                    last_quarter_count INTEGER,
                    last_quarter_avg_price_target DOUBLE PRECISION,
                    last_year_count INTEGER,
                    last_year_avg_price_target DOUBLE PRECISION,
                    all_time_count INTEGER,
                    all_time_avg_price_target DOUBLE PRECISION,
                    publishers TEXT,
                    source VARCHAR(50) DEFAULT 'FMP',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker)
                )
            """)
            
            # Create indexes for analyst_consensus
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_analyst_consensus_ticker 
                ON analyst_consensus(ticker)
            """)
            
            # Dividends table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dividends (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    dividend DOUBLE PRECISION,
                    adj_dividend DOUBLE PRECISION,
                    record_date DATE,
                    payment_date DATE,
                    declaration_date DATE,
                    source VARCHAR(50) DEFAULT 'FMP',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, date)
                )
            """)
            
            # Stock splits table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_splits (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    numerator INTEGER,
                    denominator INTEGER,
                    label VARCHAR(100),
                    source VARCHAR(50) DEFAULT 'FMP',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, date)
                )
            """)
            
            # Create indexes
            for table in ["income_statements", "balance_sheets", "cash_flow_statements", 
                          "earnings_data", "insider_trades", "institutional_holdings"]:
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table}_ticker 
                    ON {table}(ticker)
                """)
            
            conn.commit()
    
    # ==========================================
    # Income Statements
    # ==========================================
    
    def add_income_statement(self, data: Dict[str, Any]) -> bool:
        """Add or update an income statement."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO income_statements
                (ticker, date, period, revenue, gross_profit, operating_income, net_income,
                 eps, eps_diluted, cost_of_revenue, operating_expenses, interest_expense,
                 income_tax_expense, ebitda, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date, period)
                DO UPDATE SET
                    revenue = EXCLUDED.revenue,
                    gross_profit = EXCLUDED.gross_profit,
                    operating_income = EXCLUDED.operating_income,
                    net_income = EXCLUDED.net_income,
                    eps = EXCLUDED.eps,
                    eps_diluted = EXCLUDED.eps_diluted,
                    cost_of_revenue = EXCLUDED.cost_of_revenue,
                    operating_expenses = EXCLUDED.operating_expenses,
                    interest_expense = EXCLUDED.interest_expense,
                    income_tax_expense = EXCLUDED.income_tax_expense,
                    ebitda = EXCLUDED.ebitda
            """, (
                data.get("ticker", "").upper(),
                data.get("date"),
                data.get("period", "Q"),
                data.get("revenue"),
                data.get("gross_profit"),
                data.get("operating_income"),
                data.get("net_income"),
                data.get("eps"),
                data.get("eps_diluted"),
                data.get("cost_of_revenue"),
                data.get("operating_expenses"),
                data.get("interest_expense"),
                data.get("income_tax_expense"),
                data.get("ebitda"),
                data.get("source", "FMP")
            ))
            return cursor.rowcount > 0
    
    # ==========================================
    # Balance Sheets
    # ==========================================
    
    def add_balance_sheet(self, data: Dict[str, Any]) -> bool:
        """Add or update a balance sheet."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO balance_sheets
                (ticker, date, period, total_assets, total_liabilities, total_equity,
                 cash_and_equivalents, short_term_investments, total_debt, long_term_debt,
                 short_term_debt, inventory, accounts_receivable, accounts_payable,
                 retained_earnings, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date, period)
                DO UPDATE SET
                    total_assets = EXCLUDED.total_assets,
                    total_liabilities = EXCLUDED.total_liabilities,
                    total_equity = EXCLUDED.total_equity,
                    cash_and_equivalents = EXCLUDED.cash_and_equivalents,
                    short_term_investments = EXCLUDED.short_term_investments,
                    total_debt = EXCLUDED.total_debt,
                    long_term_debt = EXCLUDED.long_term_debt,
                    short_term_debt = EXCLUDED.short_term_debt,
                    inventory = EXCLUDED.inventory,
                    accounts_receivable = EXCLUDED.accounts_receivable,
                    accounts_payable = EXCLUDED.accounts_payable,
                    retained_earnings = EXCLUDED.retained_earnings
            """, (
                data.get("ticker", "").upper(),
                data.get("date"),
                data.get("period", "Q"),
                data.get("total_assets"),
                data.get("total_liabilities"),
                data.get("total_equity"),
                data.get("cash_and_equivalents"),
                data.get("short_term_investments"),
                data.get("total_debt"),
                data.get("long_term_debt"),
                data.get("short_term_debt"),
                data.get("inventory"),
                data.get("accounts_receivable"),
                data.get("accounts_payable"),
                data.get("retained_earnings"),
                data.get("source", "FMP")
            ))
            return cursor.rowcount > 0
    
    # ==========================================
    # Cash Flow Statements
    # ==========================================
    
    def add_cash_flow_statement(self, data: Dict[str, Any]) -> bool:
        """Add or update a cash flow statement."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO cash_flow_statements
                (ticker, date, period, operating_cash_flow, investing_cash_flow,
                 financing_cash_flow, free_cash_flow, capital_expenditure, dividends_paid,
                 stock_repurchased, debt_repayment, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date, period)
                DO UPDATE SET
                    operating_cash_flow = EXCLUDED.operating_cash_flow,
                    investing_cash_flow = EXCLUDED.investing_cash_flow,
                    financing_cash_flow = EXCLUDED.financing_cash_flow,
                    free_cash_flow = EXCLUDED.free_cash_flow,
                    capital_expenditure = EXCLUDED.capital_expenditure,
                    dividends_paid = EXCLUDED.dividends_paid,
                    stock_repurchased = EXCLUDED.stock_repurchased,
                    debt_repayment = EXCLUDED.debt_repayment
            """, (
                data.get("ticker", "").upper(),
                data.get("date"),
                data.get("period", "Q"),
                data.get("operating_cash_flow"),
                data.get("investing_cash_flow"),
                data.get("financing_cash_flow"),
                data.get("free_cash_flow"),
                data.get("capital_expenditure"),
                data.get("dividends_paid"),
                data.get("stock_repurchased"),
                data.get("debt_repayment"),
                data.get("source", "FMP")
            ))
            return cursor.rowcount > 0
    
    # ==========================================
    # Earnings Data
    # ==========================================
    
    def add_earnings_data(self, data: Dict[str, Any]) -> bool:
        """Add or update earnings data."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO earnings_data
                (ticker, date, eps_actual, eps_estimated, revenue_actual,
                 revenue_estimated, surprise_percent, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date)
                DO UPDATE SET
                    eps_actual = EXCLUDED.eps_actual,
                    eps_estimated = EXCLUDED.eps_estimated,
                    revenue_actual = EXCLUDED.revenue_actual,
                    revenue_estimated = EXCLUDED.revenue_estimated,
                    surprise_percent = EXCLUDED.surprise_percent
            """, (
                data.get("ticker", "").upper(),
                data.get("date"),
                data.get("eps_actual"),
                data.get("eps_estimated"),
                data.get("revenue_actual"),
                data.get("revenue_estimated"),
                data.get("surprise_percent"),
                data.get("source", "FMP")
            ))
            return cursor.rowcount > 0
    
    # ==========================================
    # Insider Trading
    # ==========================================
    
    def add_insider_trade(self, data: Dict[str, Any]) -> bool:
        """Add an insider trade."""
        with get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO insider_trades
                    (ticker, filing_date, transaction_date, insider_name, insider_title,
                     transaction_type, shares, price, value, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, filing_date, insider_name, transaction_type, shares)
                    DO NOTHING
                """, (
                    data.get("ticker", "").upper(),
                    data.get("filing_date"),
                    data.get("transaction_date"),
                    data.get("insider_name", "")[:255],
                    data.get("insider_title", "")[:255],
                    data.get("transaction_type", "")[:50],
                    data.get("shares"),
                    data.get("price"),
                    data.get("value"),
                    data.get("source", "FMP")
                ))
                return cursor.rowcount > 0
            except Exception as e:
                print(f"[FinancialStatementsStore] Insider trade error: {e}")
                return False
    
    # ==========================================
    # Institutional Holdings
    # ==========================================
    
    def add_institutional_holding(self, data: Dict[str, Any]) -> bool:
        """Add or update an institutional holding."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO institutional_holdings
                (ticker, holder_name, shares, value, weight_percent, change_shares,
                 change_percent, filing_date, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, holder_name, filing_date)
                DO UPDATE SET
                    shares = EXCLUDED.shares,
                    value = EXCLUDED.value,
                    weight_percent = EXCLUDED.weight_percent,
                    change_shares = EXCLUDED.change_shares,
                    change_percent = EXCLUDED.change_percent
            """, (
                data.get("ticker", "").upper(),
                data.get("holder_name", "")[:255],
                data.get("shares"),
                data.get("value"),
                data.get("weight_percent"),
                data.get("change_shares"),
                data.get("change_percent"),
                data.get("filing_date"),
                data.get("source", "FMP")
            ))
            return cursor.rowcount > 0
    
    # ==========================================
    # Company Profiles
    # ==========================================
    
    def add_company_profile(self, data: Dict[str, Any]) -> bool:
        """Add or update a detailed company profile."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO company_profiles
                (ticker, name, exchange, sector, industry, description, ceo, website,
                 country, city, employees, market_cap, beta, price, avg_volume,
                 ipo_date, is_actively_trading, source, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    exchange = EXCLUDED.exchange,
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry,
                    description = EXCLUDED.description,
                    ceo = EXCLUDED.ceo,
                    website = EXCLUDED.website,
                    country = EXCLUDED.country,
                    city = EXCLUDED.city,
                    employees = EXCLUDED.employees,
                    market_cap = EXCLUDED.market_cap,
                    beta = EXCLUDED.beta,
                    price = EXCLUDED.price,
                    avg_volume = EXCLUDED.avg_volume,
                    ipo_date = EXCLUDED.ipo_date,
                    is_actively_trading = EXCLUDED.is_actively_trading,
                    updated_at = EXCLUDED.updated_at
            """, (
                data.get("ticker", "").upper(),
                data.get("name"),
                data.get("exchange"),
                data.get("sector"),
                data.get("industry"),
                data.get("description"),
                data.get("ceo"),
                data.get("website"),
                data.get("country"),
                data.get("city"),
                data.get("employees"),
                data.get("market_cap"),
                data.get("beta"),
                data.get("price"),
                data.get("avg_volume"),
                data.get("ipo_date"),
                data.get("is_actively_trading", True),
                data.get("source", "FMP"),
                datetime.now()
            ))
            return cursor.rowcount > 0
    
    # ==========================================
    # ESG Scores
    # ==========================================
    
    def add_esg_score(self, data: Dict[str, Any]) -> bool:
        """Add or update ESG scores."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO esg_scores
                (ticker, date, esg_score, environmental_score, social_score,
                 governance_score, esg_risk_rating, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date)
                DO UPDATE SET
                    esg_score = EXCLUDED.esg_score,
                    environmental_score = EXCLUDED.environmental_score,
                    social_score = EXCLUDED.social_score,
                    governance_score = EXCLUDED.governance_score,
                    esg_risk_rating = EXCLUDED.esg_risk_rating
            """, (
                data.get("ticker", "").upper(),
                data.get("date") or datetime.now().date(),
                data.get("esg_score"),
                data.get("environmental_score"),
                data.get("social_score"),
                data.get("governance_score"),
                data.get("esg_risk_rating"),
                data.get("source", "FMP")
            ))
            return cursor.rowcount > 0
    
    # ==========================================
    # DCF Valuations
    # ==========================================
    
    def add_dcf_valuation(self, data: Dict[str, Any]) -> bool:
        """
        Add or update DCF valuation (latest only - one record per ticker).
        
        Uses ON CONFLICT (ticker) to ensure only the most recent DCF is stored.
        Historical DCF tracking is not needed - we use stock_prices for price trends.
        """
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO dcf_valuations
                (ticker, date, dcf_value, stock_price, upside_percent, source, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (ticker)
                DO UPDATE SET
                    date = EXCLUDED.date,
                    dcf_value = EXCLUDED.dcf_value,
                    stock_price = EXCLUDED.stock_price,
                    upside_percent = EXCLUDED.upside_percent,
                    source = EXCLUDED.source,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                data.get("ticker", "").upper(),
                data.get("date") or datetime.now().date(),
                data.get("dcf_value"),
                data.get("stock_price"),
                data.get("upside_percent"),
                data.get("source", "FMP")
            ))
            return cursor.rowcount > 0
    
    # ==========================================
    # Analyst Estimates
    # ==========================================
    
    def add_analyst_estimate(self, data: Dict[str, Any]) -> bool:
        """Add or update analyst estimates."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO analyst_estimates
                (ticker, date, estimated_revenue_avg, estimated_revenue_low,
                 estimated_revenue_high, estimated_eps_avg, estimated_eps_low,
                 estimated_eps_high, estimated_ebit_avg, estimated_net_income_avg,
                 forecast_dispersion, actual_eps, number_of_analysts_revenue, 
                 number_of_analysts_eps, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date)
                DO UPDATE SET
                    estimated_revenue_avg = EXCLUDED.estimated_revenue_avg,
                    estimated_revenue_low = EXCLUDED.estimated_revenue_low,
                    estimated_revenue_high = EXCLUDED.estimated_revenue_high,
                    estimated_eps_avg = EXCLUDED.estimated_eps_avg,
                    estimated_eps_low = EXCLUDED.estimated_eps_low,
                    estimated_eps_high = EXCLUDED.estimated_eps_high,
                    estimated_ebit_avg = EXCLUDED.estimated_ebit_avg,
                    estimated_net_income_avg = EXCLUDED.estimated_net_income_avg,
                    forecast_dispersion = EXCLUDED.forecast_dispersion,
                    actual_eps = EXCLUDED.actual_eps,
                    number_of_analysts_revenue = EXCLUDED.number_of_analysts_revenue,
                    number_of_analysts_eps = EXCLUDED.number_of_analysts_eps,
                    source = EXCLUDED.source
            """, (
                data.get("ticker", "").upper(),
                data.get("date"),
                data.get("estimated_revenue_avg"),
                data.get("estimated_revenue_low"),
                data.get("estimated_revenue_high"),
                data.get("estimated_eps_avg"),
                data.get("estimated_eps_low"),
                data.get("estimated_eps_high"),
                data.get("estimated_ebit_avg"),
                data.get("estimated_net_income_avg"),
                data.get("forecast_dispersion"),
                data.get("actual_eps"),
                data.get("number_of_analysts_revenue"),
                data.get("number_of_analysts_eps"),
                data.get("source", "FMP")
            ))
            return cursor.rowcount > 0
    
    def get_analyst_estimates(self, ticker: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get analyst estimates for a ticker."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    date, estimated_revenue_avg, estimated_revenue_low, estimated_revenue_high,
                    estimated_eps_avg, estimated_eps_low, estimated_eps_high,
                    estimated_ebit_avg, estimated_net_income_avg, forecast_dispersion,
                    actual_eps, number_of_analysts_revenue, number_of_analysts_eps
                FROM analyst_estimates
                WHERE ticker = %s
                ORDER BY date DESC
                LIMIT %s
            """, (ticker.upper(), limit))
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    # ==========================================
    # Analyst Consensus
    # ==========================================
    
    def add_analyst_consensus(self, data: Dict[str, Any]) -> bool:
        """Add or update analyst consensus data (grades, price targets, summary)."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO analyst_consensus
                (ticker, strong_buy, buy, hold, sell, strong_sell, consensus_rating,
                 target_high, target_low, target_consensus, target_median,
                 last_month_count, last_month_avg_price_target,
                 last_quarter_count, last_quarter_avg_price_target,
                 last_year_count, last_year_avg_price_target,
                 all_time_count, all_time_avg_price_target, publishers, source, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (ticker)
                DO UPDATE SET
                    strong_buy = EXCLUDED.strong_buy,
                    buy = EXCLUDED.buy,
                    hold = EXCLUDED.hold,
                    sell = EXCLUDED.sell,
                    strong_sell = EXCLUDED.strong_sell,
                    consensus_rating = EXCLUDED.consensus_rating,
                    target_high = EXCLUDED.target_high,
                    target_low = EXCLUDED.target_low,
                    target_consensus = EXCLUDED.target_consensus,
                    target_median = EXCLUDED.target_median,
                    last_month_count = EXCLUDED.last_month_count,
                    last_month_avg_price_target = EXCLUDED.last_month_avg_price_target,
                    last_quarter_count = EXCLUDED.last_quarter_count,
                    last_quarter_avg_price_target = EXCLUDED.last_quarter_avg_price_target,
                    last_year_count = EXCLUDED.last_year_count,
                    last_year_avg_price_target = EXCLUDED.last_year_avg_price_target,
                    all_time_count = EXCLUDED.all_time_count,
                    all_time_avg_price_target = EXCLUDED.all_time_avg_price_target,
                    publishers = EXCLUDED.publishers,
                    source = EXCLUDED.source,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                data.get("ticker", "").upper(),
                data.get("strong_buy", 0),
                data.get("buy", 0),
                data.get("hold", 0),
                data.get("sell", 0),
                data.get("strong_sell", 0),
                data.get("consensus_rating"),
                data.get("target_high"),
                data.get("target_low"),
                data.get("target_consensus"),
                data.get("target_median"),
                data.get("last_month_count"),
                data.get("last_month_avg_price_target"),
                data.get("last_quarter_count"),
                data.get("last_quarter_avg_price_target"),
                data.get("last_year_count"),
                data.get("last_year_avg_price_target"),
                data.get("all_time_count"),
                data.get("all_time_avg_price_target"),
                data.get("publishers"),
                data.get("source", "FMP")
            ))
            return cursor.rowcount > 0
    
    def get_analyst_consensus(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get analyst consensus data for a ticker."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    ticker, strong_buy, buy, hold, sell, strong_sell, consensus_rating,
                    target_high, target_low, target_consensus, target_median,
                    last_month_count, last_month_avg_price_target,
                    last_quarter_count, last_quarter_avg_price_target,
                    last_year_count, last_year_avg_price_target,
                    all_time_count, all_time_avg_price_target, publishers,
                    updated_at
                FROM analyst_consensus
                WHERE ticker = %s
            """, (ticker.upper(),))
            row = cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))
            return None
    
    def get_analyst_consensus_batch(self, tickers: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get analyst consensus data for multiple tickers."""
        if not tickers:
            return {}
        
        with get_connection() as conn:
            cursor = conn.cursor()
            placeholders = ','.join(['%s'] * len(tickers))
            cursor.execute(f"""
                SELECT 
                    ticker, strong_buy, buy, hold, sell, strong_sell, consensus_rating,
                    target_high, target_low, target_consensus, target_median,
                    last_month_count, last_month_avg_price_target,
                    last_quarter_count, last_quarter_avg_price_target,
                    last_year_count, last_year_avg_price_target,
                    all_time_count, all_time_avg_price_target, publishers,
                    updated_at
                FROM analyst_consensus
                WHERE ticker IN ({placeholders})
            """, [t.upper() for t in tickers])
            columns = [desc[0] for desc in cursor.description]
            results = {}
            for row in cursor.fetchall():
                data = dict(zip(columns, row))
                results[data['ticker']] = data
            return results
    
    # ==========================================
    # Dividends
    # ==========================================
    
    def add_dividend(self, data: Dict[str, Any]) -> bool:
        """Add or update dividend data."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO dividends
                (ticker, date, dividend, adj_dividend, record_date, payment_date,
                 declaration_date, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date)
                DO UPDATE SET
                    dividend = EXCLUDED.dividend,
                    adj_dividend = EXCLUDED.adj_dividend,
                    record_date = EXCLUDED.record_date,
                    payment_date = EXCLUDED.payment_date,
                    declaration_date = EXCLUDED.declaration_date
            """, (
                data.get("ticker", "").upper(),
                data.get("date"),
                data.get("dividend"),
                data.get("adj_dividend"),
                data.get("record_date"),
                data.get("payment_date"),
                data.get("declaration_date"),
                data.get("source", "FMP")
            ))
            return cursor.rowcount > 0
    
    # ==========================================
    # Stock Splits
    # ==========================================
    
    def add_stock_split(self, data: Dict[str, Any]) -> bool:
        """Add or update stock split data."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO stock_splits
                (ticker, date, numerator, denominator, label, source)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date)
                DO UPDATE SET
                    numerator = EXCLUDED.numerator,
                    denominator = EXCLUDED.denominator,
                    label = EXCLUDED.label
            """, (
                data.get("ticker", "").upper(),
                data.get("date"),
                data.get("numerator"),
                data.get("denominator"),
                data.get("label"),
                data.get("source", "FMP")
            ))
            return cursor.rowcount > 0
    
    # ==========================================
    # Retrieval Methods
    # ==========================================
    
    def get_latest_dcf(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest DCF valuation for a ticker.
        
        Optimized: Since we only store one record per ticker (latest only),
        this is a simple lookup without ordering.
        """
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT dcf_value, stock_price, upside_percent, date 
                FROM dcf_valuations 
                WHERE ticker = %s
            """, (ticker.upper(),))
            row = cursor.fetchone()
            if row:
                return {
                    "dcf_value": row[0],
                    "stock_price": row[1],
                    "upside_percent": row[2],
                    "date": row[3]
                }
            return None

    def get_income_statement_history(self, ticker: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Get historical income statements."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT date, period, revenue, net_income, eps, operating_income 
                FROM income_statements 
                WHERE ticker = %s 
                ORDER BY date DESC LIMIT %s
            """, (ticker.upper(), limit))
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_latest_growth_metrics(self, ticker: str) -> Dict[str, Any]:
        """Get the most recent growth metrics."""
        # Note: These are currently stored in the general financial_metrics table 
        # in MetricsStore, but some might be here in the future.
        return {}

    # ==========================================
    # Statistics
    # ==========================================
    
    def get_stats(self) -> Dict[str, int]:
        """Get database statistics."""
        with get_connection() as conn:
            cursor = conn.cursor()
            stats = {}
            
            tables = [
                "income_statements", "balance_sheets", "cash_flow_statements",
                "earnings_data", "insider_trades", "institutional_holdings",
                "company_profiles", "esg_scores", "dcf_valuations",
                "analyst_estimates", "analyst_consensus", "dividends", "stock_splits"
            ]
            
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    stats[table] = cursor.fetchone()[0]
                except:
                    stats[table] = 0
            
            return stats


# Singleton instance
_statements_store: Optional[FinancialStatementsStore] = None


def get_financial_statements_store() -> FinancialStatementsStore:
    """Get or create the singleton FinancialStatementsStore instance."""
    global _statements_store
    if _statements_store is None:
        _statements_store = FinancialStatementsStore()
    return _statements_store

