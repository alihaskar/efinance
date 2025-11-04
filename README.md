# exfinance

[![PyPI version](https://badge.fury.io/py/exfinance.svg)](https://badge.fury.io/py/exfinance)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python package to automate downloading high-quality tick data from Exness data archives (ex2archive).

## Features
- List all available trading pairs
- Download historical tick data for any pair and date range
- Save data as CSV or load directly as pandas DataFrame
- Configurable data source URL

## Installation

Install from PyPI:
```bash
pip install exfinance
```

Or using Poetry:
```bash
poetry add exfinance
```

### Development Installation
```bash
git clone https://github.com/alihaskar/efinance.git
cd efinance
poetry install
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
