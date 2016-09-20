import luigi
import os
import pandas as pd

from .BaseDataFrameHDF import BaseDataFrameHDF
from .config import time_map


class CountsPerTPDataFrameHDF(luigi.Task):

    working_dir = luigi.Parameter()
    time_period = luigi.Parameter()
    hdf_out_name = luigi.Parameter(default='count_{}.h5'.format(time_period))
    hdf_out_key = luigi.Parameter(default='count_{}'.format(time_period))

    def requires(self):
        return BaseDataFrameHDF(self.working_dir)

    def run(self):

        pd.\
            read_hdf(self.requires()).\
            set_index('date_col').\
            groupby(pd.TimeGrouper(self.time_period)).\
            sum().\
            fillna(0).\
            reset_index().\
            to_hdf(self.output(), self.hdf_out_key)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.working_dir, self.hdf_out_name))

