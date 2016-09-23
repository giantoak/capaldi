import luigi
import os
import pandas as pd

from .BaseDataFrameCSV import BaseDataFrameCSV


class CountsPerTPDataFrameCSV(luigi.Task):

    working_dir = luigi.Parameter()
    time_period = luigi.Parameter()
    out_fname = luigi.Parameter(default='count_{}.csv'.format(time_period))

    def requires(self):
        return BaseDataFrameCSV(self.working_dir)

    def run(self):

        with self.input().open('r') as infile:
            df = pd.read_csv(infile, parse_dates=['date_col'])

        df = df.\
            set_index('date_col').\
            groupby(pd.TimeGrouper(self.time_period)).\
            sum().\
            fillna(0).\
            reset_index()

        with self.output().open('w') as outfile:
            df.to_csv(outfile, index=False)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.working_dir, self.out_fname))
