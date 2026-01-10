# Financial Data Mapping Documentation

This document provides a comprehensive mapping of all financial statements and growth metrics stored in the SmartStock AI database, including their FMP API field names, database column names, descriptions, and significance.

---

## 1. FINANCIAL STATEMENTS

### Table Information
- **Database Tables**: 
  - `income_statements`
  - `balance_sheets`
  - `cash_flow_statements`
- **FMP Endpoints**: 
  - `/stable/income-statement`
  - `/stable/balance-sheet-statement`
  - `/stable/cash-flow-statement`
- **Data Frequency**: **Quarterly (Q)** and **Annual (FY)**
- **Historical Coverage**: Last 20 quarters (5 years)

---

### 1.1 INCOME STATEMENTS

| Metric Name | FMP Field Name | Database Column | Category | Description | Significance |
|------------|----------------|----------------|----------|-------------|--------------|
| Revenue | `revenue` | `revenue` | INCOME_STATEMENT | Total sales or income generated from business operations | Primary indicator of company size and growth. Top-line metric that drives all other financial metrics. |
| Gross Profit | `grossProfit` | `gross_profit` | INCOME_STATEMENT | Revenue minus cost of goods sold (COGS) | Measures production efficiency and pricing power. Higher gross profit indicates better cost control or premium pricing. |
| Cost of Revenue | `costOfRevenue` | `cost_of_revenue` | INCOME_STATEMENT | Direct costs attributable to goods/services sold | Key for calculating gross margin. Rising costs may indicate supply chain issues or inflation pressure. |
| Operating Expenses | `operatingExpenses` | `operating_expenses` | INCOME_STATEMENT | Total expenses from normal business operations (excluding COGS) | Includes R&D, SG&A. Critical for understanding operational efficiency and cost management. |
| Operating Income | `operatingIncome` | `operating_income` | INCOME_STATEMENT | Gross profit minus operating expenses | Core profitability metric. Shows how well the company generates profit from operations before interest/taxes. |
| EBITDA | `ebitda` | `ebitda` | INCOME_STATEMENT | Earnings Before Interest, Taxes, Depreciation, and Amortization | Proxy for operating cash flow. Used for valuation multiples (EV/EBITDA). Less affected by accounting choices. |
| Interest Expense | `interestExpense` | `interest_expense` | INCOME_STATEMENT | Cost of borrowing money (debt interest payments) | Indicates debt burden. High interest expense relative to operating income signals financial stress. |
| Income Tax Expense | `incomeTaxExpense` | `income_tax_expense` | INCOME_STATEMENT | Taxes paid on taxable income | Effective tax rate analysis. Lower rates may indicate tax optimization or geographic advantages. |
| Net Income | `netIncome` | `net_income` | INCOME_STATEMENT | Total profit after all expenses, interest, and taxes | Bottom-line profitability. Primary driver of earnings per share and shareholder returns. |
| EPS (Basic) | `eps` | `eps` | INCOME_STATEMENT | Earnings Per Share (net income / shares outstanding) | Most widely watched metric. Directly impacts stock price. Used in P/E ratio calculations. |
| EPS (Diluted) | `epsdiluted` | `eps_diluted` | INCOME_STATEMENT | EPS accounting for all potential shares (options, warrants) | More conservative than basic EPS. Important for companies with significant stock-based compensation. |

---

### 1.2 BALANCE SHEETS

| Metric Name | FMP Field Name | Database Column | Category | Description | Significance |
|------------|----------------|----------------|----------|-------------|--------------|
| Total Assets | `totalAssets` | `total_assets` | BALANCE_SHEET | Sum of all company assets (current + non-current) | Measures company size and resource base. Used in asset turnover ratios. |
| Cash and Cash Equivalents | `cashAndCashEquivalents` | `cash_and_equivalents` | BALANCE_SHEET | Liquid assets (cash, short-term investments) | Financial flexibility indicator. High cash = ability to invest, pay dividends, or weather downturns. |
| Short-Term Investments | `shortTermInvestments` | `short_term_investments` | BALANCE_SHEET | Marketable securities maturing within 1 year | Part of liquid assets. Shows cash management strategy and liquidity buffer. |
| Accounts Receivable | `netReceivables` | `accounts_receivable` | BALANCE_SHEET | Money owed to company by customers (net of allowances) | Indicates sales collection efficiency. Rising receivables may signal collection issues or aggressive sales. |
| Inventory | `inventory` | `inventory` | BALANCE_SHEET | Goods held for sale or raw materials | Working capital component. High inventory may indicate slow sales or overproduction. |
| Accounts Payable | `accountPayables` | `accounts_payable` | BALANCE_SHEET | Money owed by company to suppliers | Working capital management. Longer payment terms improve cash flow but may strain supplier relationships. |
| Total Debt | `totalDebt` | `total_debt` | BALANCE_SHEET | Sum of short-term and long-term debt | Leverage indicator. High debt increases financial risk but can amplify returns. |
| Short-Term Debt | `shortTermDebt` | `short_term_debt` | BALANCE_SHEET | Debt due within 1 year | Liquidity risk indicator. High short-term debt requires strong cash flow or refinancing ability. |
| Long-Term Debt | `longTermDebt` | `long_term_debt` | BALANCE_SHEET | Debt due after 1 year | Capital structure component. Used in debt-to-equity and interest coverage ratios. |
| Total Liabilities | `totalLiabilities` | `total_liabilities` | BALANCE_SHEET | Sum of all company obligations | Financial obligations measure. Used in debt-to-assets ratio to assess solvency risk. |
| Total Equity | `totalStockholdersEquity` | `total_equity` | BALANCE_SHEET | Assets minus liabilities (book value) | Shareholder ownership value. Used in P/B ratio. Negative equity indicates financial distress. |
| Retained Earnings | `retainedEarnings` | `retained_earnings` | BALANCE_SHEET | Cumulative profits not paid as dividends | Growth financing indicator. Shows how much profit is reinvested vs. distributed to shareholders. |

---

### 1.3 CASH FLOW STATEMENTS

| Metric Name | FMP Field Name | Database Column | Category | Description | Significance |
|------------|----------------|----------------|----------|-------------|--------------|
| Operating Cash Flow | `operatingCashFlow` | `operating_cash_flow` | CASH_FLOW | Cash generated from core business operations | Most important cash flow metric. Positive OCF is essential for business sustainability. |
| Investing Cash Flow | `netCashUsedForInvestingActivites` | `investing_cash_flow` | CASH_FLOW | Cash used for investments (capex, acquisitions, securities) | Typically negative (cash outflow). Shows growth investments. High negative = aggressive expansion. |
| Financing Cash Flow | `netCashUsedProvidedByFinancingActivities` | `financing_cash_flow` | CASH_FLOW | Cash from/for financing (debt, equity, dividends) | Positive = raising capital. Negative = paying down debt or returning capital to shareholders. |
| Free Cash Flow | `freeCashFlow` | `free_cash_flow` | CASH_FLOW | Operating cash flow minus capital expenditures | Key valuation metric. Shows cash available for dividends, buybacks, debt repayment, or growth. |
| Capital Expenditure | `capitalExpenditure` | `capital_expenditure` | CASH_FLOW | Cash spent on long-term assets (property, plant, equipment) | Growth investment indicator. High capex may signal expansion but reduces free cash flow. |
| Dividends Paid | `dividendsPaid` | `dividends_paid` | CASH_FLOW | Cash distributed to shareholders as dividends | Shareholder return measure. Consistent dividends indicate financial stability and shareholder-friendly policy. |
| Stock Repurchased | `commonStockRepurchased` | `stock_repurchased` | CASH_FLOW | Cash used to buy back company shares | Shareholder return mechanism. Reduces shares outstanding, increases EPS. Signals management confidence. |
| Debt Repayment | `debtRepayment` | `debt_repayment` | CASH_FLOW | Cash used to pay down debt principal | Financial discipline indicator. Reducing debt improves balance sheet strength and reduces interest expense. |

---

## 2. FINANCIAL GROWTH METRICS

### Table Information
- **Database Table**: `financial_metrics` (with `metric_name` filtering for growth metrics)
- **FMP Endpoint**: `/stable/financial-growth`
- **Data Frequency**: **Quarterly (Q)** and **Annual (FY)**
- **Historical Coverage**: Last 20 quarters (5 years)
- **Note**: Growth metrics are stored as percentages (e.g., 0.0643 = 6.43% growth)

---

### 2.1 GROWTH METRICS TABLE

| Metric Name | FMP Field Name | Database Column | Category | Description | Significance |
|------------|----------------|----------------|----------|-------------|--------------|
| Revenue Growth | `revenueGrowth` | `revenue_growth` | INCOME_STATEMENT | Year-over-year percentage change in revenue | Primary growth indicator. Shows if company is "getting bigger." Sustained growth is key for growth stocks. |
| Gross Profit Growth | `grossProfitGrowth` | `gross_profit_growth` | INCOME_STATEMENT | Year-over-year percentage change in gross profit | Profitability expansion indicator. Faster than revenue growth = improving margins. |
| EBITDA Growth | `ebitdaGrowth` | `ebitda_growth` | INCOME_STATEMENT | Year-over-year percentage change in EBITDA | Operating profitability growth. Shows if company is "getting richer." Key for valuation and cash flow analysis. |
| Operating Income Growth | `operatingIncomeGrowth` | `operating_income_growth` | INCOME_STATEMENT | Year-over-year percentage change in operating income | Core profitability growth. Excludes interest/taxes, focuses on operational efficiency. |
| Net Income Growth | `netIncomeGrowth` | `net_income_growth` | INCOME_STATEMENT | Year-over-year percentage change in net income | Bottom-line growth. Most comprehensive profitability growth metric. |
| EPS Growth | `epsgrowth` | `eps_growth` | INCOME_STATEMENT | Year-over-year percentage change in earnings per share | Per-share profitability growth. Accounts for share count changes. Critical for stock valuation. |
| EPS Diluted Growth | `epsdilutedGrowth` | `eps_diluted_growth` | INCOME_STATEMENT | Year-over-year percentage change in diluted EPS | More conservative EPS growth. Important for companies with significant stock-based compensation. |
| R&D Expense Growth | `rdexpenseGrowth` | `rd_expense_growth` | INCOME_STATEMENT | Year-over-year percentage change in research & development expenses | Innovation investment indicator. High R&D growth may signal future product pipeline but reduces current profits. |
| SG&A Expenses Growth | `sgaexpensesGrowth` | `sga_expenses_growth` | INCOME_STATEMENT | Year-over-year percentage change in selling, general & administrative expenses | Cost management indicator. Slower growth than revenue = operating leverage. Faster growth = cost inflation. |
| Total Assets Growth | `assetGrowth` | `asset_growth` | BALANCE_SHEET | Year-over-year percentage change in total assets | Balance sheet expansion. High growth may indicate aggressive expansion or acquisitions. |
| Receivables Growth | `receivablesGrowth` | `receivables_growth` | BALANCE_SHEET | Year-over-year percentage change in accounts receivable | Collection efficiency indicator. Faster than revenue growth may signal collection issues or aggressive sales. |
| Inventory Growth | `inventoryGrowth` | `inventory_growth` | BALANCE_SHEET | Year-over-year percentage change in inventory | Inventory management indicator. Faster than revenue growth may signal slow sales or overproduction. |
| Debt Growth | `debtGrowth` | `debt_growth` | BALANCE_SHEET | Year-over-year percentage change in total debt | Leverage trend indicator. High debt growth increases financial risk but may fund growth. |
| Book Value Per Share Growth | `bookValueperShareGrowth` | `book_value_per_share_growth` | BALANCE_SHEET | Year-over-year percentage change in book value per share | Shareholder equity growth per share. Positive growth indicates value creation. |
| Operating Cash Flow Growth | `operatingCashFlowGrowth` | `operating_cash_flow_growth` | CASH_FLOW | Year-over-year percentage change in operating cash flow | Cash generation growth. Critical for sustainability. Positive growth = improving cash generation. |
| Free Cash Flow Growth | `freeCashFlowGrowth` | `free_cash_flow_growth` | CASH_FLOW | Year-over-year percentage change in free cash flow | Discretionary cash growth. Key for dividend sustainability, buybacks, and debt repayment. |
| Dividend Per Share Growth | `dividendsperShareGrowth` | `dividend_per_share_growth` | OTHER | Year-over-year percentage change in dividends per share | Shareholder return growth. Consistent growth signals financial strength and shareholder-friendly policy. |

---

## 3. DATA USAGE NOTES

### 3.1 Period Types
- **Q**: Quarterly data (Q1, Q2, Q3, Q4)
- **FY**: Annual/Fiscal Year data
- **TTM**: Trailing Twelve Months (calculated metric, not directly from FMP)

### 3.2 Growth Metric Format
- Growth metrics are stored as **decimals** (e.g., 0.0643 = 6.43% growth)
- Some metrics may be stored as percentages (e.g., 6.43) depending on FMP API response
- Always check `metric_unit` column for "%" to confirm format

### 3.3 Unique Constraints
- **Financial Statements**: `(ticker, date, period)` - ensures no duplicate statements
- **Growth Metrics**: `(ticker, metric_name, period, period_end_date)` - allows same metric for different periods

### 3.4 Data Freshness
- Data is ingested via automated daily sync pipeline
- Financial statements: Last 20 quarters (5 years)
- Growth metrics: Last 20 quarters (5 years)
- Historical data is preserved (not deleted when new data arrives)

---

## 4. QUERY EXAMPLES

### Get Latest Income Statement for AAPL
```sql
SELECT * FROM income_statements 
WHERE ticker = 'AAPL' 
ORDER BY date DESC 
LIMIT 1;
```

### Get Revenue Growth Trends for AAPL (Last 5 Quarters)
```sql
SELECT period_end_date, metric_value 
FROM financial_metrics 
WHERE ticker = 'AAPL' 
  AND metric_name = 'revenue_growth' 
  AND period = 'Q'
ORDER BY period_end_date DESC 
LIMIT 5;
```

### Compare Free Cash Flow Across Companies
```sql
SELECT ticker, date, free_cash_flow 
FROM cash_flow_statements 
WHERE date = (SELECT MAX(date) FROM cash_flow_statements)
ORDER BY free_cash_flow DESC;
```

---

## 5. METRIC CATEGORIES

All metrics are categorized in the `metric_categories` table for better organization and agent intelligence:

### Category Definitions:
- **INCOME_STATEMENT**: Metrics from income statements and related growth rates (revenue, profit, EPS)
- **BALANCE_SHEET**: Metrics from balance sheets and related growth rates (assets, liabilities, equity)
- **CASH_FLOW**: Metrics from cash flow statements and related growth rates (operating cash flow, free cash flow)
- **KEY_METRICS**: Valuation and efficiency ratios (P/E, P/B, ROE, ROIC, etc.)
- **YIELDS**: Return metrics (dividend yield, earnings yield, free cash flow yield)
- **OTHER**: Miscellaneous metrics that don't fit other categories

### Category Usage:
This categorization helps the SmartStock AI agent:
- Understand which metrics to use for different types of analysis
- Group related metrics together for comparative analysis
- Filter metrics by category when answering specific questions
- Provide context-aware recommendations based on metric types

---

**Last Updated**: 2026-01-09  
**Data Source**: Financial Modeling Prep (FMP) API  
**Ingestion Scripts**: 
- `scripts/ingest_financial_statements.py`
- `scripts/ingest_financial_growth_metrics.py`

