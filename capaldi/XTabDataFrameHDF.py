import luigi
import os
import pandas as pd

from .BaseDataFrameHDF import BaseDataFrameHDF
from .config import time_map


class XTabDataFrameHDF(luigi.Task):

    working_dir = luigi.Parameter()
    hdf_out_name = luigi.Parameter(default='base_xtab.h5')
    hdf_out_key = luigi.Parameter(default='base_xtab')

    def requires(self):
        return BaseDataFrameHDF(self.working_dir)

    def run(self):

        df = pd.read_hdf(self.requires())

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

        df.to_hdf(self.output(), self.hdf_out_key)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.working_dir, self.hdf_out_name))
