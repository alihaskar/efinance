# exfinance

A Python package to automate downloading high-quality tick data from Exness data archives (ex2archive).

## Features
- List all available trading pairs
- Download historical tick data for any pair and date range
- Save data as CSV or load directly as pandas DataFrame
- Configurable data source URL

## Installation

### Using Poetry
```bash
poetry install
```

### Using pip
```bash
pip install git@github.com:alihaskar/efinance.git
```

## Usage
```python
from exfinance import Exness
exness = Exness()
pairs = exness.get_available_pairs()
data = exness.download('EURUSD', '2023-01-01', '2023-03-01')
```

## Downloader Optimizations
- Downloads are now parallelized for faster multi-month fetches (uses ThreadPoolExecutor).
- CSVs are read directly from zip files in memory if you don't specify a save path (no disk I/O).
- Error handling is granular: network, extraction, and parsing errors are logged separately.
- Date range validation: start date must not be after end date; monthly frequency uses last day of month.
- Logging is always configured for consistent output.

## Linting
This project uses [ruff](https://github.com/astral-sh/ruff) for linting:
```bash
poetry run ruff check .
```

## License
MIT
