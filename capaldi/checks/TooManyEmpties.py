import luigi
import numpy as np

from .BaseCapaldiCheck import BaseCapaldiCheck

from capaldi.checks.check_values import MAX_EMPTY_PROPORTION


class TooManyEmpties(BaseCapaldiCheck):
    label = 'TooManyEmpties'
    output_prefix = luigi.Parameter(default='TooManyEmpties')

    def check(self, df):

        result_dict = dict()

        empty_buckets = np.sum(df.count_col.values < 1)
        if empty_buckets / df.shape[0] >= MAX_EMPTY_PROPORTION:
            result_dict['result'] = True
            result_dict['empty_warning'] = 'Too many empty buckets'

        else:
            result_dict['result'] = False

        return result_dict
