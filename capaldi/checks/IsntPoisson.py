import ujson as json
import luigi
import numpy as np
import numpy.random as npr
import os
import pandas as pd
from scipy.stats import chisquare

from .BaseCapaldiCheck import BaseCapaldiCheck

from capaldi.checks.check_values import POISSON_P_CUTOFF


class IsntPoisson(BaseCapaldiCheck):

    output_prefix = luigi.Parameter(default='IsntPoisson')
    cols_to_use = luigi.ListParameter(default=[])

    def requires(self):
        return self.df_to_use

    def check(self, df):
        xtab_vals = np.ravel(df)
        sim_vals = npr.poisson(np.mean(xtab_vals), len(xtab_vals))
        p_val = chisquare(xtab_vals, sim_vals).pvalue

        result_dict = dict()

        if p_val < POISSON_P_CUTOFF:
            result_dict['result'] = True
            result_dict['p_warning'] = \
                'Null hypothesis rejected: {} < {}'.format(p_val,
                                                           POISSON_P_CUTOFF)
        else:
            result_dict['result'] = False

        return result_dict

    def run(self):

        with self.input().open('r') as infile:
            xtab = pd.read_csv(infile, columns=self.cols_to_use)

        result_dict = self.check(xtab)

        with self.output().open('wb') as outfile:
            json.dump(result_dict, outfile)
