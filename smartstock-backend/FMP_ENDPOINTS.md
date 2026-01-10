# FMP API Endpoints for Key Metrics Testing

## Current Endpoint We're Using

### Key Metrics (Annual) - 10 Years
```
GET https://financialmodelingprep.com/stable/key-metrics
```

**Parameters:**
- `symbol`: GOOGL (or any ticker symbol)
- `period`: annual
- `limit`: 10 (for 10 years of data)
- `apikey`: YOUR_API_KEY

**Full URL Example:**
```
https://financialmodelingprep.com/stable/key-metrics?symbol=GOOGL&period=annual&limit=10&apikey=YOUR_API_KEY
```

**Curl Command:**
```bash
curl "https://financialmodelingprep.com/stable/key-metrics?symbol=GOOGL&period=annual&limit=10&apikey=YOUR_API_KEY"
```

---

## Alternative Endpoints to Check for Missing Metrics

### 1. Financial Ratios (Annual)
```
GET https://financialmodelingprep.com/stable/ratios/{symbol}
```

**Parameters:**
- `period`: annual
- `limit`: 10
- `apikey`: YOUR_API_KEY

**Full URL:**
```
https://financialmodelingprep.com/stable/ratios/GOOGL?period=annual&limit=10&apikey=YOUR_API_KEY
```

**What it might provide:**
- P/E Ratio (peRatioTTM)
- P/B Ratio (pbRatioTTM)
- P/S Ratio (priceToSalesRatioTTM)
- ROE, ROA, ROIC
- Debt ratios, margins, etc.

---

### 2. Financial Ratios TTM (Trailing Twelve Months)
```
GET https://financialmodelingprep.com/stable/ratios-ttm/{symbol}
```

**Parameters:**
- `apikey`: YOUR_API_KEY

**Full URL:**
```
https://financialmodelingprep.com/stable/ratios-ttm/GOOGL?apikey=YOUR_API_KEY
```

**Note:** TTM endpoints typically return only the latest snapshot, not historical data.

---

### 3. Key Metrics TTM
```
GET https://financialmodelingprep.com/stable/key-metrics-ttm/{symbol}
```

**Parameters:**
- `apikey`: YOUR_API_KEY

**Full URL:**
```
https://financialmodelingprep.com/stable/key-metrics-ttm/GOOGL?apikey=YOUR_API_KEY
```

---

### 4. Enterprise Values (Historical)
```
GET https://financialmodelingprep.com/stable/enterprise-values/{symbol}
```

**Parameters:**
- `period`: annual
- `limit`: 10
- `apikey`: YOUR_API_KEY

**Full URL:**
```
https://financialmodelingprep.com/stable/enterprise-values/GOOGL?period=annual&limit=10&apikey=YOUR_API_KEY
```

---

## Testing in Python

```python
import requests
import json

FMP_API_KEY = "YOUR_API_KEY"
symbol = "GOOGL"

# Test Key Metrics
url = f"https://financialmodelingprep.com/stable/key-metrics"
params = {
    "symbol": symbol,
    "period": "annual",
    "limit": 10,
    "apikey": FMP_API_KEY
}
response = requests.get(url, params=params)
data = response.json()
print(json.dumps(data[0] if data else {}, indent=2))

# Test Financial Ratios
url2 = f"https://financialmodelingprep.com/stable/ratios/{symbol}"
params2 = {
    "period": "annual",
    "limit": 10,
    "apikey": FMP_API_KEY
}
response2 = requests.get(url2, params=params2)
data2 = response2.json()
print(json.dumps(data2[0] if data2 else {}, indent=2))
```

---

## Current Status

**What `/key-metrics` endpoint provides:**
- ✅ `currentRatio` - Available
- ✅ `freeCashFlowYield` - Available  
- ✅ `earningsYield` - Available (now storing)

**What `/key-metrics` endpoint does NOT provide:**
- ❌ `peRatio`, `pbRatio`, `psRatio`
- ❌ `roe`, `roa`, `roic`
- ❌ `quickRatio`, `debtToEquity`, `debtToAssets`
- ❌ `grossProfitMargin`, `operatingProfitMargin`, `netProfitMargin`
- ❌ `interestCoverage`, `inventoryTurnover`, `receivablesTurnover`

**Recommendation:** Test the `/ratios` endpoint to see if it provides these missing metrics with historical data (10 years).


