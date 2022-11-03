import numpy as np 
import pandas as pd 
import urllib.request
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime

class exness():
    def __init__(self):
        
        self.get_data()
        
        
    def get_data(self):
        '''
        get data from ex2archive
        get_data will return a list of all available pairs and assets on ex2archive
        '''

        url = 'https://ticks.ex2archive.com/ticks/'
        uf = urllib.request.urlopen(url)
        html = uf.read()
        html = html.decode("utf-8")
        html = html.split('\r\n')
        html[2].split()[1].split(':')[1].split(',')[0].split('"')

        cols = []

        for i in range(1, len(html)-1):
            raw = html[i].split()[1].split(':')[1].split(',')[0].split('"')[1]
            cols.append(raw)

        self.cols = cols
        
        return self.cols

    def parse_dates(self, start=None, end=None):

        '''
        parse the start and end dates into datetime objects
        the date format should be year-month-day:
        ex:
        '2022-10-31' 
        '''
        self.start = start
        self.end = end
        
        if self.end is None:
            self.end = datetime.today()

        #self.start = datetime.strptime(self.start, '%Y-%m-%d')
        #self.end = datetime.strptime(self.end, '%Y-%m-%d')
        
        self.dates = pd.date_range(self.start, self.end, freq='m')
        
        return self.dates
        
    def download(self, pair, start, end):
        '''
        returns a DataFrame Object for the selected pair 
        start = start date  
        end = end date
        dates should be either a year or in the format of 'year-month-day' --> '2022-10-23'
        '''
        self.pair = pair.upper()

        self.parse_dates()
        
        files = []
        for i in range(len(self.dates)):
            url = f'https://ticks.ex2archive.com/ticks/{self.pair}/{self.dates[i].year}/{datetime.strftime(self.dates[i], "%m")}/Exness_{self.pair}_{self.dates[i].year}_{datetime.strftime(self.dates[i], "%m")}.zip'
            print(f'downloading: {self.pair} | {datetime.strftime(self.dates[i], "%m")}')
            http_response = urlopen(url)
            zipfile = ZipFile(BytesIO(http_response.read()))
            zipfile.extractall(path='')
            file = f'Exness_{self.pair}_{self.dates[i].year}_{datetime.strftime(self.dates[i], "%m")}.csv'
            files.append(file)
        
        data = pd.DataFrame()
        
        for file in files:
            f = pd.read_csv(file, parse_dates=True, index_col=['Timestamp'])
            data = data.append(f)
        self.data = data
        
        return self.data