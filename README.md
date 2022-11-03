# efinance
A library to automate downloading quality tick data from Exness data archives

+ Project under construction

------

from exfinance.downloader import exness

exness = exness()

- exness.get_data() --> returns a list of all available assets data archive 
- exness.download(pair, start, end) --> download data frame of the selected dates
