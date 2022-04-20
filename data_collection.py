import datetime
import os.path
import pytz
from typing import Union

import pandas as pd
import yfinance as yf

COINS = ['BTC-USD', 'ETH-USD', 'BNB-USD', 'XRP-USD', 'SOL-USD',
         'LUNA-USD', 'ADA-USD', 'AVAX-USD', 'DOGE-USD', 'SHIB-USD']

OUTPUT_FILE = 'cryptocurrencies_data.csv'


class DataCollector:
    def __init__(self, coins: list[str],
                 start_date: Union[str, datetime.datetime] = '2009-01-03',
                 appending: bool = False):
        self.coins = coins
        self.start_date = start_date
        self.appending = appending
        self.coin_data = ''

    def get_data(self):
        todays_date = datetime.datetime.now(pytz.timezone('Europe/Amsterdam'))
        last_sunday_offset = todays_date.weekday() + 1
        last_sunday = todays_date - datetime.timedelta(days=last_sunday_offset)

        self.coin_data = yf.download(
            self.coins, start=self.start_date, end=last_sunday,
            interval='1d', group_by='tickers'
        )

    def writing_to_file(self):
        if not self.coin_data:
            self.get_data()
        coin_data = pd.DataFrame(self.coin_data)
        if self.appending:
            coin_data.to_csv(OUTPUT_FILE, mode='a', header=False)
        else:
            coin_data.to_csv(OUTPUT_FILE)


def main():
    if os.path.isfile(OUTPUT_FILE):
        with open(OUTPUT_FILE) as coin_data:
            for line in coin_data:
                pass
            last_line = line
        last_date, _ = last_line.strip().split()

        collector = DataCollector(COINS, start_date=last_date, appending=True)
        collector.writing_to_file()
    else:
        collector = DataCollector(COINS)
        collector.writing_to_file()


if __name__ == '__main__':
    main()
