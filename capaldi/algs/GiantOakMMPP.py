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
    hdf_out_key = luigi.Parameter(default='mmpp')

    def requires(self):
        return {'file': etl.XTabDataFrameCSV(self.working_dir),
                'error_checks': {
                    'isnt_poisson': checks.IsntPoisson(
                        etl.XTabDataFrameCSV(self.working_dir),
                        [self.count_col,
                         self.time_col,
                         self.time_col_two])
                  }
                }

    def run(self):

        # Only run if wday column exists.
        # An issue to be fixed with the MMPP.
        if self.time_col != 'wday' and self.time_col_two != 'wday':
            return

        for error_key in self.requires()['error_checks']:
            jsn = json.loads(open(self.requires()[error_key]).read())
            if not jsn['result']:
                self.write_result(jsn)
                return

        with self.input()['file'].open('r') as infile:
            xtab = pd.pivot_table(pd.read_csv(infile,
                                              usecols=[self.count_col,
                                                       self.time_col,
                                                       self.time_col_two]),
                                  self.count_col,
                                  self.time_col,
                                  self.time_col_two,
                                  aggfunc=sum).\
                fillna(0)


        # Transpose data - need to check MMPP
        # to confirm that this can be removed.
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
            result_dict = {'error': r.txt}

        else:
            r_json = r.json()
            result_dict = {key: pd.DataFrame(np.
                                             array(r_json[key]).
                                             reshape(xtab.shape),
                                             index=xtab.index,
                                             columns=xtab.columns)
                           for key in r_json}

        self.write_result_to_hdf5('{}_{}'.format(self.time_col,
                                                 self.time_col_two),
                                  result_dict)
