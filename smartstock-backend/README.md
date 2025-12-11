# SmartStock AI Backend

Agentic RAG API for Financial Analysis powered by FastAPI, LangChain, and LangGraph.

## Quick Start with uv

```bash
# Install dependencies
uv sync

# Run the development server
uv run python main.py

# Or use uvicorn directly
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## API Endpoints

- `GET /` - Health check
- `POST /api/ask` - Submit a financial query

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

