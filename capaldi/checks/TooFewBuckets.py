import luigi
import os
import pandas as pd

from .check_values import MIN_BUCKETS


class TooFewBuckets(luigi.Task):

    working_dir = luigi.Parameter()
    df_to_use = luigi.TaskParameter()
    output_suffix = luigi.Parameter()
    output_fname = luigi.Parameter(default='TooManyEmpties_{}.json'.format(output_suffix))

    def requires(self):
        return self.df_to_use

    def run(self):
        df = pd.read_hdf(self.requires())

        result_dict = dict()

        if df.shape[0] < MIN_BUCKETS:
            result_dict['result'] = True
            result_dict['bucket_warning'] = 'Not enough buckets'
        else:
            result_dict['result'] = False

        with open(self.output(), 'wb') as outfile:
            json.dump(result_dict, outfile)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.working_dir, self.output_fname))
