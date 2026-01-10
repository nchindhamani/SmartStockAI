# SmartStock AI Backend

Agentic RAG API for Financial Analysis powered by FastAPI, LangChain, and LangGraph.

## Quick Start

See **[QUICK_START.md](QUICK_START.md)** for detailed setup instructions.

**Quick Setup:**
```bash
# Install dependencies
uv sync --all-extras

# Configure environment variables
cp .env.example .env  # Edit with your API keys

# Run the development server
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

**Health Check:**
```bash
curl http://localhost:8000/api/health
```

## API Endpoints

- `GET /api/health` - Enhanced health check with data quality metrics
- `POST /api/ask` - Submit a financial query to the agent
- `GET /api/company/{ticker}` - Get company information and metrics
- `GET /api/compare` - Compare financial metrics across companies

See [QUICK_START.md](QUICK_START.md) for detailed API usage and examples.

## Project Structure

```
smartstock-backend/
├── main.py          # FastAPI application
├── models.py        # Pydantic schemas
├── pyproject.toml   # Project configuration
└── uv.lock          # Locked dependencies
```

## Development

```bash
# Install with dev dependencies
uv sync --extra dev

# Run tests
uv run pytest

# Format code
uv run ruff format .

# Lint code
uv run ruff check .
```

## Phase 2: LangChain Integration

```bash
# Install LangChain dependencies
uv sync --extra langchain
```

