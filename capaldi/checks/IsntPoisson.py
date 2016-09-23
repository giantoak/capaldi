import ujson as json
import luigi
import numpy as np
import numpy.random as npr
import os
import pandas as pd
from scipy.stats import chisquare

from capaldi.checks.check_values import POISSON_P_CUTOFF


class IsntPoisson(luigi.Task):

    working_dir = luigi.Parameter()
    df_to_use = luigi.TaskParameter()
    cols_to_use = luigi.ListParameter(default=[])
    output_fname = luigi.Parameter(default='IsntPoisson_{}.json'.format(cols_to_use))

    def requires(self):
        return self.df_to_use

    def run(self):

        with self.input().open('r') as infile:
            xtab = pd.read_csv(infile, columns=self.cols_to_use)

        xtab_vals = np.ravel(xtab)
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

        with self.output().open('wb') as outfile:
            json.dump(result_dict, outfile)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.working_dir, self.output_fname))
