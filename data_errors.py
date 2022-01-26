from datetime import datetime
import pandas as pd
import os

# directory for the error files
path = '/home/jimmy/Desktop/data_quality/'


class MarketPhaseErrorParser:

    def __init__(self):
        self.vendor = None
        self.market = None
        self.dates = []
        self.mkt_phases = []
        self.syds = []

    def read_file(self, filename):
        file = open(f'{filename}', 'r')
        lines = file.readlines()

        self.vendor = lines[0].split("||")[-2]
        self.market = filename.split("mphase_", 1)[1].partition('_')[0]

        for line in lines:
            date = datetime.strptime(line.split("||")[2], '%Y-%m-%d_%H:%M:%S.%f').strftime('%Y-%m-%d')
            self.dates.append(date)
            mkt_phase = line.split("Market_Phase => ", 1)[1].partition(',')[0]
            self.mkt_phases.append(mkt_phase)
            syd = line.split("||")[5]
            self.syds.append(syd)

    def to_df(self):
        df = pd.DataFrame(
            {'market': self.market,
             'vendor': self.vendor,
             'date': self.dates,
             'market_phase': self.mkt_phases,
             'syd': self.syds
             })

        df = df.groupby(['market', 'vendor', 'date', 'market_phase'])
        df = df.agg({"syd": "nunique"}).reset_index()
        df = df.rename(columns={'syd': 'num_of_syds'})

        return df


def main():
    final_df = pd.DataFrame()
    for filename in os.listdir(path):
        f = os.path.join(path, filename)
        if os.path.isfile(f):
            parser = MarketPhaseErrorParser()
            parser.read_file(f)
            final_df = final_df.append(parser.to_df())

    print(final_df)
    final_df.to_csv("~/Desktop/extra/mkt_phase_errors.csv", index=False)


if __name__ == "__main__":
    main()
