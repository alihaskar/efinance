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

## Linting
This project uses [ruff](https://github.com/astral-sh/ruff) for linting:
```bash
poetry run ruff check .
```

## License
MIT
