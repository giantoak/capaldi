import ujson as json
import luigi
import numpy as np
import pandas as pd

from .BaseCapaldiAlg import BaseCapaldiAlg

import capaldi.etl as etl
import capaldi.checks as checks

from capaldi.algs.opencpu_support import r_array_fmt
from capaldi.algs.opencpu_support import opencpu_url_fmt
from capaldi.algs.opencpu_support import request_with_retries


class GiantOakMMPP(BaseCapaldiAlg):
    time_col_two = luigi.Parameter()

    def requires(self):
        cur_df = etl.XTabDataFrameCSV(working_dir=self.working_dir)
        return {'file': cur_df,
                'error_checks': {
                    'isnt_poisson': checks.IsntPoisson(
                        cur_df,
                        ['count_col',
                         self.time_col,
                         self.time_col_two])
                  }
                }

    def load_dataframe(self):
        with self.input()['file'].open('r') as infile:
            xtab = pd.pivot_table(pd.read_csv(infile,
                                              usecols=['count_col',
                                                       self.time_col,
                                                       self.time_col_two]),
                                  'count_col',
                                  self.time_col,
                                  self.time_col_two,
                                  aggfunc=sum).\
                fillna(0)

        return xtab

    def alg(self, xtab):

        # Transpose data
        # Need to check MMPP to confirm that this can be removed.
        if self.time_col != 'wday':
            xtab = xtab.T

        xtab_vals = ','.join([str(x) for x in np.ravel(xtab)])

        data_str = r_array_fmt(xtab_vals,
                               xtab.shape[1],
                               xtab.shape[0])

        url = opencpu_url_fmt('library',  # 'github', 'giantoak',
                              'mmppr',
                              'R',
                              'sensorMMPP',
                              'json')

        params = {'N': data_str}

        r = request_with_retries([url, params])

        if not r.ok:
            return {'error': r.txt}

        r_json = r.json()
        return {key: json.loads(pd.DataFrame(np.array(r_json[key]).reshape(xtab.shape),
                                             index=xtab.index,
                                             columns=xtab.columns).to_json())
                for key in r_json()}

    def run(self):

        result_dict = None

        # Verify that wday is in the data.
        # Need to fix MMPP so that this can be removed
        if self.time_col != 'wday' and self.time_col_two != 'wday':
            result_dict = {'error': 'No "wday" field'}

        else:
            for error_check in self.input()['error_checks']:
                with error_check.open('r') as infile:
                    jsn = json.load(infile)
                    if not jsn['result']:
                        result_dict = jsn
                        break

        if result_dict is None:
            df = self.load_dataframe()
            result_dict = self.alg(df)

        for key in self.wrapper_keys:
            result_dict = {key: result_dict}

        with self.output().open('wb') as outfile:
            json.dump(result_dict, outfile)
