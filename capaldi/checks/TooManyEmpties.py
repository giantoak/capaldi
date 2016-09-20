import ujson as json
import luigi
import numpy as np
import os
import pandas as pd

from .check_values import MAX_EMPTY_PROPORTION


class TooManyEmpties(luigi.Task):

    working_dir = luigi.Parameter()
    df_to_use = luigi.TaskParameter()
    output_suffix = luigi.Parameter()
    output_fname = luigi.Parameter(default='TooManyEmpties_{}.json'.format(output_suffix))

    def requires(self):
        return self.df_to_use

    def run(self):
        df = pd.read_hdf(self.requires())

        result_dict = dict()

        empty_buckets = np.sum(df.count_col.values < 1)
        if empty_buckets / df.shape[0] >= MAX_EMPTY_PROPORTION:
            result_dict['result'] = True
            result_dict['empty_warning'] = 'Too many empty buckets'

        else:
            result_dict['result'] = False

        with open(self.output(), 'wb') as outfile:
            json.dump(result_dict, outfile)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.working_dir, self.output_fname))
