# SmartStock AI

Natural-language interface over a **2,400+ ticker** U.S. equity dataset. Ask a question; a **LangGraph** agent picks a tool, reads **PostgreSQL** (and SEC filings / news in **ChromaDB**), and returns a cited answer with structured metrics — not an unconstrained LLM essay.

Example questions:

- Compare AAPL vs. MSFT on growth, margins, and DCF
- Summarize key risks from GOOGL’s latest 10-Q
- What moved NVDA more than 5% last week, and which headlines sat in that window?

---

## Features

**Agent that routes, then retrieves.** A router classifies each query as earnings, multi-name comparison, or price-move analysis, then runs only that tool. Follow-ups stay on the same per-user chat thread.

**Three research tools**

| | What it does |
| --- | --- |
| **Earnings / filings** | Pulls 10-Q, 10-K, or earnings-call excerpts from the vector store, plus statement metrics. Synthesis is required to use retrieved text only. |
| **Comparison** | Screens or compares names on revenue growth, P/E, DCF upside, and related fundamentals. Supports “better buy?” and index-style stock picks. |
| **Price + news** | Finds days with large % moves, then attaches news **inside a ±24 hour window** of each move so causality claims are time-bounded. |

**Grounded output.** Inline citations `[1]`, `[2]` map to filings, prices, or headlines. Prompts forbid filling gaps with general knowledge; missing fields are called out instead of invented.

**Accounts.** Email and password sign-in. Passwords are stored as **bcrypt hashes** in PostgreSQL. Sessions are JWTs (httpOnly cookie plus bearer token). Chat threads are scoped per user. Agent and market APIs require a signed-in user.

**Research UI.** Next.js chat with markdown synthesis, color-coded metric snapshots, clickable citation popovers, and source links.

**Data platform, not a single API call.** Financial Modeling Prep (and optional Finnhub) land in Postgres: daily OHLC, income/balance/cash flow, analyst ratings and estimates, earnings surprises, company profiles, and DCF. Incremental price sync skips weekends and U.S. market holidays.

**Scheduled ingest.** GitHub Actions refresh the warehouse **daily** (prices, DCF, Russell 2000 list), **weekly** (statements, surprises, analyst data), and **monthly** (profiles, growth metrics).

**Operability.** A health endpoint reports component status and completeness (profiles, prices, statements, analyst coverage) so empty or stale tables are visible.

---

## How a query runs

```
User question
    → LangGraph router  (earnings | comparison | price_news)
    → Tool
         • SQL: prices, statements, DCF, analyst data
         • Chroma: SEC excerpts / news embeddings
         • Temporal join: volatile days ↔ news ±24h
    → Synthesizer (Gemini, or OpenAI)
    → JSON: synthesis + metrics_snapshot + citations
    → Chat UI
```

```
smartstock-frontend/     Next.js 14 chat UI
smartstock-backend/      FastAPI + LangGraph agent, tools, stores
.github/workflows/       daily / weekly / monthly ingest
```

**Stack:** Next.js, React, TypeScript, FastAPI, LangGraph, LangChain, Gemini, PostgreSQL, ChromaDB, FMP, GitHub Actions.

---

## Data in Postgres

| Domain | Tables (examples) |
| --- | --- |
| Market | `stock_prices` (OHLC) |
| Fundamentals | `income_statements`, `balance_sheets`, `cash_flow_statements`, `financial_metrics` |
| Street | `analyst_ratings`, `analyst_estimates`, `earnings_surprises` |
| Valuation / identity | `dcf_valuations`, `company_profiles` |
| News | `news_articles` (short retention) |
| Auth | `users` (email + bcrypt hash) |

---

## API surface

| | | |
| --- | --- | --- |
| Auth | `POST /api/auth/register`, `/login`, `/logout` · `GET /api/auth/me` | |
| Agent | `POST /api/ask` | signed-in |
| Market | `GET /api/company/{ticker}` · `GET /api/compare` | signed-in |
| Ops | `GET /api/health` | public |

## License

All rights reserved.
