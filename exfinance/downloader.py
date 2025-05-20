import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import List, Optional, Union
from urllib.request import urlopen
from zipfile import ZipFile

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')

logger = logging.getLogger(__name__)

class Exness:
    """Downloader for financial data from ex2archive."""

    def __init__(self, base_url: str = 'https://ticks.ex2archive.com/ticks/'):
        """
        Initialize the Exness downloader and fetch available pairs.
        Args:
            base_url: Base URL for the ex2archive tick data.
        """
        self.BASE_URL = base_url
        self.available_pairs = self._fetch_available_pairs()

    def _fetch_available_pairs(self) -> List[str]:
        """
        Fetch list of all available pairs from ex2archive.
        Returns:
            List[str]: List of available trading pairs
        """
        with urlopen(self.BASE_URL) as response:
            html = response.read().decode("utf-8").split('\r\n')
        pairs = []
        for line in html[1:-1]:  # Skip header and empty last line
            try:
                pair = line.split()[1].split(':')[1].split(',')[0].strip('"')
                pairs.append(pair)
            except Exception as e:
                logger.warning(f"Failed to parse pair from line: {line} ({e})")
        return pairs

    def get_available_pairs(self) -> List[str]:
        """
        Get list of available trading pairs.
        Returns:
            List[str]: List of available trading pairs
        """
        return self.available_pairs

    def _parse_dates(self, start: Union[str, datetime],
                     end: Optional[Union[str, datetime]] = None) -> pd.DatetimeIndex:
        """
        Parse start and end dates into a range of months (last day of each month).
        Args:
            start: Start date in 'YYYY-MM-DD' format or datetime object
            end: End date in 'YYYY-MM-DD' format or datetime object (defaults to today)
        Returns:
            pd.DatetimeIndex: Range of months (last day of each month) between start and end dates
        Raises:
            ValueError: If start > end
        """
        if isinstance(start, str):
            start = pd.to_datetime(start)
        if end is None:
            end = datetime.today()
        elif isinstance(end, str):
            end = pd.to_datetime(end)
        if start > end:
            raise ValueError(f"Start date {start} is after end date {end}.")
        return pd.date_range(start, end, freq='M')

    def download(self, pair: str, start: Union[str, datetime],
                end: Optional[Union[str, datetime]] = None,
                save_path: Optional[Union[str, Path]] = None) -> pd.DataFrame:
        """
        Download historical data for a specific trading pair.
        Args:
            pair: Trading pair symbol (e.g., 'EURUSD')
            start: Start date in 'YYYY-MM-DD' format or datetime object
            end: End date in 'YYYY-MM-DD' format or datetime object (defaults to today)
            save_path: Optional path to save downloaded files (defaults to current directory)
        Returns:
            pd.DataFrame: Historical data for the specified pair and date range
        Raises:
            ValueError: If the pair is not available or no data is downloaded.
        """
        pair = pair.upper()
        if pair not in self.available_pairs:
            raise ValueError(f"Pair '{pair}' not available. Use get_available_pairs() to see available options.")
        dates = self._parse_dates(start, end)
        save_path = Path(save_path) if save_path else None
        data_frames = []
        errors = []

        def fetch_and_parse(date):
            url = (f"{self.BASE_URL}{pair}/{date.year}/"
                  f"{date.strftime('%m')}/Exness_{pair}_{date.year}_{date.strftime('%m')}.zip")
            logger.info(f"Downloading: {pair} | {date.strftime('%Y-%m')} from {url}")
            try:
                with urlopen(url) as response:
                    zip_bytes = BytesIO(response.read())
            except Exception as e:
                logger.error(f"Network error for {pair} {date.strftime('%Y-%m')}: {e}")
                errors.append((date, 'network', e))
                return None
            try:
                with ZipFile(zip_bytes) as zip_file:
                    csv_name = f"Exness_{pair}_{date.year}_{date.strftime('%m')}.csv"
                    if save_path:
                        zip_file.extractall(path=save_path)
                        file_path = save_path / csv_name
                        df = pd.read_csv(file_path, parse_dates=['Timestamp'], index_col=['Timestamp'])
                    else:
                        with zip_file.open(csv_name) as csvfile:
                            df = pd.read_csv(csvfile, parse_dates=['Timestamp'], index_col=['Timestamp'])
                    return df
            except KeyError as e:
                logger.error(f"Extraction error (missing CSV) for {pair} {date.strftime('%Y-%m')}: {e}")
                errors.append((date, 'extraction', e))
            except Exception as e:
                logger.error(f"Parsing error for {pair} {date.strftime('%Y-%m')}: {e}")
                errors.append((date, 'parsing', e))
            return None

        with ThreadPoolExecutor() as executor:
            future_to_date = {executor.submit(fetch_and_parse, date): date for date in dates}
            for future in as_completed(future_to_date):
                df = future.result()
                if df is not None:
                    data_frames.append(df)

        if not data_frames:
            raise ValueError(f"No data was downloaded for {pair} in the specified period: {dates[0]} to {dates[-1]}")
        return pd.concat(data_frames, axis=0)
