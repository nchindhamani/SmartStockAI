# Earnings Surprises - Data Requirements

## Summary for FMP API Endpoint Search

When checking FMP API endpoints for earnings surprises data, you need to find endpoints that provide the following fields:

---

## ✅ REQUIRED FIELDS (Minimum)

These 4 fields are **absolutely essential**:

1. **symbol** (ticker)
   - Field names to check: `"symbol"`, `"ticker"`, `"companySymbol"`
   - Type: String (e.g., "AAPL", "TSLA")

2. **date** (earnings report date)
   - Field names to check: `"date"`, `"reportDate"`, `"earningsDate"`, `"announcementDate"`
   - Type: Date string (YYYY-MM-DD format)

3. **epsActual** (actual reported EPS)
   - Field names to check: `"epsActual"`, `"eps"`, `"actualEps"`, `"reportedEps"`, `"actualEPS"`
   - Type: Float/Double (e.g., 1.58, 0.44)

4. **epsEstimated** (analyst consensus estimate for EPS)
   - Field names to check: `"epsEstimated"`, `"estimatedEps"`, `"epsEstimate"`, `"consensusEps"`, `"estimatedEPS"`
   - Type: Float/Double (e.g., 1.52, 0.42)

---

## ⚪ OPTIONAL FIELDS (Nice to Have)

These fields enhance the data but are not required:

5. **revenueActual** (actual reported revenue)
   - Field names to check: `"revenueActual"`, `"revenue"`, `"actualRevenue"`, `"reportedRevenue"`
   - Type: Float/Double (in dollars, e.g., 89498000000)

6. **revenueEstimated** (analyst consensus estimate for revenue)
   - Field names to check: `"revenueEstimated"`, `"estimatedRevenue"`, `"revenueEstimate"`, `"consensusRevenue"`
   - Type: Float/Double (in dollars, e.g., 88700000000)

7. **period** (fiscal quarter/period)
   - Field names to check: `"period"`, `"quarter"`, `"fiscalPeriod"`, `"reportPeriod"`
   - Type: String (e.g., "Q1", "Q2", "Q3", "Q4", "FY")

8. **fiscalYear** (fiscal year)
   - Field names to check: `"fiscalYear"`, `"year"`, `"fiscalYearEnd"`
   - Type: Integer (e.g., 2024, 2025)

---

## 📊 Example API Response Structure

What we expect the API response to look like:

```json
{
  "symbol": "AAPL",
  "date": "2024-09-30",
  "epsActual": 1.58,
  "epsEstimated": 1.52,
  "revenueActual": 89498000000,
  "revenueEstimated": 88700000000,
  "period": "Q4",
  "fiscalYear": 2024
}
```

Or as an array of records:

```json
[
  {
    "symbol": "AAPL",
    "date": "2024-09-30",
    "epsActual": 1.58,
    "epsEstimated": 1.52,
    "revenueActual": 89498000000,
    "revenueEstimated": 88700000000
  },
  {
    "symbol": "TSLA",
    "date": "2024-09-30",
    "epsActual": 0.44,
    "epsEstimated": 0.42,
    "revenueActual": 25371000000,
    "revenueEstimated": 24800000000
  }
]
```

---

## 🔍 FMP API Endpoints to Check

When searching FMP documentation, look for these endpoint patterns:

1. `/earnings-calendar` (or `/earnings_calendar`)
2. `/earnings-surprises` (or `/earnings_surprises`)
3. `/earnings-announcements` (or `/earnings_announcements`)
4. `/earnings` (generic earnings endpoint)
5. `/historical-earnings` (or `/historical_earnings`)
6. `/quarterly-earnings` (or `/quarterly_earnings`)

Check both `/stable/` and `/v3/` and `/v4/` versions of each endpoint.

---

## 📝 Key Points

### What We Calculate

The script automatically calculates:
- **surprise_percent** = `((actual - estimated) / estimated) * 100`
  - Positive value = Beat (actual > estimated)
  - Negative value = Miss (actual < estimated)
  - Zero = Met expectations (actual = estimated)

### What We Store

The `earnings_data` table stores:
- `ticker` (from symbol)
- `date` (from date)
- `eps_actual` (from epsActual)
- `eps_estimated` (from epsEstimated)
- `revenue_actual` (from revenueActual, optional)
- `revenue_estimated` (from revenueEstimated, optional)
- `surprise_percent` (calculated)
- `source` (set to "FMP")

### Script Flexibility

The script can be easily modified to handle different field names. If you find an endpoint with different field names (e.g., `"actualEps"` instead of `"epsActual"`), we can update the field mapping in the `transform_earnings_record()` function.

---

## ✅ Quick Checklist for Testing Endpoints

When testing an FMP endpoint, verify it provides:

- [ ] **symbol** field (or similar ticker identifier)
- [ ] **date** field (earnings report date)
- [ ] **epsActual** or similar (actual EPS value)
- [ ] **epsEstimated** or similar (estimated EPS value)
- [ ] Records contain both actual AND estimated values (not just one or the other)
- [ ] Data is for reported earnings (not just future estimates)

---

## 🎯 Minimum Viable Data

**Absolute minimum** to make the script work:
1. symbol
2. date  
3. epsActual
4. epsEstimated

With just these 4 fields, we can:
- Store earnings surprises data
- Calculate surprise percentages
- Track beats and misses
- Answer queries about earnings performance

Revenue fields are bonus data that enhance analysis but are not required.

