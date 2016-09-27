import luigi
import os
import pandas as pd

from .BaseDataFrameCSV import BaseDataFrameCSV

from capaldi.config import time_map


class XTabDataFrameCSV(luigi.Task):

    working_dir = luigi.Parameter()
    out_fname = luigi.Parameter(default='base_xtab.csv')

    def requires(self):
        return BaseDataFrameCSV(working_dir=self.working_dir)

    def run(self):

        with self.input().open('r') as infile:
            df = pd.read_csv(infile, parse_dates=['date_col'])

        df.date_col = pd.to_datetime(df.date_col)

        df['hhour'] = df.date_col.apply(lambda x: x.hour * 2 + int(x.minute * 2. / 60))
        df['hour'] = df.date_col.apply(lambda x: x.hour)
        df['wday'] = df.date_col.apply(lambda x: x.weekday())
        df['mday'] = df.date_col.apply(lambda x: x.day)
        df['yweek'] = df.date_col.apply(lambda x: x.week)
        df['mweek'] = df.date_col.apply(lambda x: int(x.day / 7) + 1)
        df['month'] = df.date_col.apply(lambda x: x.month)
        df['year'] = df.date_col.apply(lambda x: x.year)

        for col in time_map:
            df[col] = df[col].astype('category',
                                     categories=list(range(time_map[col])),
                                     ordered=True)

        df.year = df.year.astype('category',
                                 categories=list(range(df.year.min(), df.year.max() + 1)),
                                 ordered=True)

        with self.output().open('w') as outfile:
            df.to_csv(outfile, index=False)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.working_dir, self.out_fname))
