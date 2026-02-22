# Codebase Instructions

This repository contains a personal financial tracking application using **NiceGUI**, **Pandas**, **DuckDB**, and **Ledger CLI**.

## 1. Environment & Build

- **Package Manager:** `uv` is used for dependency management.
- **Python Version:** >= 3.12 (as per `pyproject.toml`).
- **Dependencies:** Managed in `pyproject.toml` and `uv.lock`.
- **Database:** Uses `DuckDB` (file-based) and `Valkey` (Redis fork) via `walrus`.

### Common Commands

- **Install Dependencies:**
  ```bash
  uv sync
  ```
- **Run Application:**
  ```bash
  uv run app.py
  ```
- **Run Hourly Script:**
  ```bash
  uv run finance_hourly.py
  ```
- **Docker:**
  - Build: `task dev-build` (via `Taskfile.yml`)
  - The project is designed to run in a containerized environment (Arch Linux base).

### Testing & Verification

- **Run All Tests:**
  ```bash
  uv run pytest
  ```
- **Run Single Test:**
  ```bash
  uv run pytest path/to/test.py::test_function_name
  ```
- **Manual Verification:** Run the relevant script (e.g., `app.py` for UI changes, `finance_hourly.py` for logic) and check for runtime errors.
- **Linting:** The project uses `ruff` for linting and formatting.
  ```bash
  uv run ruff check .
  uv run ruff format .
  uv run ruff check . --fix
  ```

### Deployment

- **After checking in new code, run:**
  ```bash
  go-task prod-sync
  ```

## 2. Code Style & Conventions

### General

- **Type Hints:** Strictly use Python type hints (`typing`, `collections.abc`).
  - Example: `def get_tickers(ts: Iterable[str]) -> dict[str, float]:`
  - Use `from __future__ import annotations` when needed for forward references.
- **Imports:** Order imports in three groups with a blank line between each:
  1. Standard library imports
  2. Third-party library imports
  3. Local application imports
- **Docstrings:** Use concise docstrings for modules, classes, and functions.
- **Logging:** Use `loguru` for logging.
  - `from loguru import logger`
  - `logger.info("Message {var}", var=value)`
- **Path Handling:** Use absolute paths or `pathlib.Path` relative to the project root. Constants are defined in `common.py` (e.g., `CODE_DIR`, `PUBLIC_HTML`).

### Project Structure

- **`app.py`**: Main entry point for the NiceGUI web application.
- **`common.py`**: Shared utilities, constants, database connections (`DuckDB`, `Walrus`/Redis), and Schwab API client.
- **`finance_*.py`**: Cron jobs/scripts for data fetching and processing.
- **`ledger_*.py`**: Interactions with Ledger CLI.
- **`web/`**: Static assets and database files.

### Database & Concurrency

- **Locking:** Access to resources is protected by locks in `common.py`.
  - **DuckDB:** Always use the context manager:
    ```python
    with common.duckdb_lock(read_only=True) as con:
        df = con.sql("SELECT * FROM ...").df()
    ```
  - **Schwab API:** Use `@common.schwab_lock` decorator or lock for API calls to prevent rate limiting/concurrency issues.

### Async/Await Patterns (NiceGUI)

- The UI (`nicegui`) is async. Database calls or IO-bound tasks should be handled carefully.
- Use `run.io_bound()` for blocking operations:
  ```python
  from nicegui import run
  result = await run.io_bound(lambda: blocking_function())
  ```
- Use `ui.on_exception` for graceful error handling in UI code.

### Naming Conventions

- **Classes:** `PascalCase` (e.g., `IncomeExpenseGraphs`)
- **Functions/Variables:** `snake_case` (e.g., `get_yearly_chart`)
- **Constants:** `UPPER_CASE` (e.g., `DUCKDB_LOCK_NAME`)
- **Files:** `snake_case.py`

### Error Handling

- Use specific exceptions where possible. Define custom exceptions when appropriate.
- In UI code, handle exceptions gracefully or use `ui.on_exception`.
- Tracebacks are often shown in the UI using `traceback.format_exc()`.
- Log errors with context before raising or handling them.

### Pandas Conventions

- Use type hints for DataFrame parameters and returns where applicable.
- Prefer method chaining when readable.
- Always specify column types explicitly when creating DataFrames from scratch.
- Handle NaN/None values explicitly before operations that might fail on them.

### Data Classes & Named Tuples

- Use `@dataclass` for structured data containers.
- Use `typing.NamedTuple` for immutable data structures (e.g., `TickerOption`, `FutureQuote`).
- Use `StrEnum` for string-based enums (e.g., `Brokerage`).
