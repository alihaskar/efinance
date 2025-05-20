import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import List, Optional, Union
from urllib.request import urlopen
from zipfile import ZipFile

import pandas as pd

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
        Parse start and end dates into a range of months.
        Args:
            start: Start date in 'YYYY-MM-DD' format or datetime object
            end: End date in 'YYYY-MM-DD' format or datetime object (defaults to today)
        Returns:
            pd.DatetimeIndex: Range of months between start and end dates
        """
        if isinstance(start, str):
            start = pd.to_datetime(start)
        if end is None:
            end = datetime.today()
        elif isinstance(end, str):
            end = pd.to_datetime(end)
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
        save_path = Path(save_path) if save_path else Path.cwd()
        data_frames = []
        for date in dates:
            url = (f"{self.BASE_URL}{pair}/{date.year}/"
                  f"{date.strftime('%m')}/Exness_{pair}_{date.year}_{date.strftime('%m')}.zip")
            logger.info(f"Downloading: {pair} | {date.strftime('%Y-%m')} from {url}")
            try:
                with urlopen(url) as response:
                    with ZipFile(BytesIO(response.read())) as zip_file:
                        zip_file.extractall(path=save_path)
                file_path = save_path / f"Exness_{pair}_{date.year}_{date.strftime('%m')}.csv"
                df = pd.read_csv(file_path, parse_dates=['Timestamp'], index_col=['Timestamp'])
                data_frames.append(df)
            except Exception as e:
                logger.error(f"Error downloading {pair} for {date.strftime('%Y-%m')}: {e}")
                continue
        if not data_frames:
            raise ValueError(f"No data was downloaded for {pair} in the specified period: {dates[0]} to {dates[-1]}")
        return pd.concat(data_frames, axis=0)
