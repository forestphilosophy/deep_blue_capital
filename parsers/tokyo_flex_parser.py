import pandas as pd
import re
from datetime import datetime


class TokyoFlexParser:

    def __init__(self, filename):
        self.filename = filename
        self.timestamps = []
        self.filename = filename
        self.quotes_df = pd.DataFrame(columns=['timestamp', 'bid', 'bid_size', 'ask', 'ask_size'])

    def get_prices(self):

        file = open(f'{self.filename}', 'r')
        lines = file.readlines()

        for line in lines:
            if 'Message_Length' in line:
                self.timestamps.append(line.split("\t")[0])


            else:
                if 'TAG_CURRENT_PRICE' in line:
                    if datetime.strptime(self.timestamps[-1][:19], '%Y-%m-%dT%H:%M:%S').hour == 2:
                        intraday_close = int(
                            re.search('Unit_Flag => 4, Integral_Part => "(.*)", Decimal_Part =>', line).group(
                                1).strip())
                    if datetime.strptime(self.timestamps[-1][:19], '%Y-%m-%dT%H:%M:%S').hour == 6:
                        close_price = int(
                            re.search('Unit_Flag => 4, Integral_Part => "(.*)", Decimal_Part =>', line).group(
                                1).strip())
                    elif datetime.strptime(self.timestamps[-1][:19],
                                           '%Y-%m-%dT%H:%M:%S').hour == 0 and 'XP_Change_Flag => 4' in line:
                        open_price = int(
                            re.search('Unit_Flag => 4, Integral_Part => "(.*)", Decimal_Part =>', line).group(
                                1).strip())

                elif 'TAG_BID_QUOTE' in line:

                    try:
                        bid = int(
                            re.search('Unit_Flag => 4, Integral_Part => "(.*)", Decimal_Part =>', line).group(
                                1).strip())
                    except:
                        bid = ""

                    try:
                        bid_size = int(
                            re.search('Unit_Flag => 0, Integral_Part => "(.*)", Sign => +', line).group(1).split(
                                '"')[
                                0].strip())
                    except:
                        bid_size = ""

                    self.quotes_df = self.quotes_df.append(
                        {'timestamp': self.timestamps[-1], 'bid': bid, 'bid_size': bid_size, 'ask': "", 'ask_size': ""},
                        ignore_index=True)

                elif 'TAG_ASK_QUOTE' in line:

                    try:
                        ask = int(
                            re.search('Unit_Flag => 4, Integral_Part => "(.*)", Decimal_Part =>', line).group(
                                1).strip())
                    except:
                        ask = ""

                    try:
                        ask_size = int(
                            re.search('Unit_Flag => 0, Integral_Part => "(.*)", Sign => +', line).group(1).split('"')[
                                0].strip())
                    except:
                        ask_size = ""

                    self.quotes_df = self.quotes_df.append(
                        {'timestamp': self.timestamps[-1], 'bid': "", 'bid_size': "", 'ask': ask, 'ask_size': ask_size},
                        ignore_index=True)

        return open_price, intraday_close, close_price


def main():
    # output text file from flex_dump
    filename = '/home/jimmy/Desktop/tokyo_flex/6599.txt'
    parser = TokyoFlexParser(filename)
    open_price, intraday_close, close_price = parser.get_prices()
    print(f"opening_price: {open_price}\nintraday_close: {intraday_close}\nclose_price: {close_price}")


if __name__ == "__main__":
    main()
