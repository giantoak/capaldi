import ujson as json
import luigi
import pandas as pd

from .BaseCapaldiAlg import BaseCapaldiAlg

import capaldi.etl as etl
import capaldi.checks as checks

from capaldi.algs.opencpu_support import dictified_json
from capaldi.algs.opencpu_support import opencpu_url_fmt
from capaldi.algs.opencpu_support import unorthodox_request_with_retries
from capaldi.algs.opencpu_support import r_list_fmt


class BaseBCP(BaseCapaldiAlg):
    hdf_out_key = luigi.Parameter(default='bcp')

    def requires(self):
        cur_df = etl.CountsPerTPDataFrameCSV(self.working_dir,
                                             self.time_col)

        return {'file': cur_df,
                'error_checks': {
                    'too_few_buckets': checks.TooFewBuckets(
                        self.working_dir,
                        cur_df,
                        self.time_col),
                    'too_many_empties': checks.TooManyEmpties(
                        self.working_dir,
                        cur_df,
                        self.time_col)
                  }
                }

    def run(self):
        for error_key in self.input()['error_checks']:
            jsn = json.loads(open(self.input()[error_key]).read())
            if not jsn['result']:
                self.write_result(jsn)
                return

        with self.input()['file'].open('r') as infile:
            df = pd.read_csv(infile, parse_dates=['date_col'])

        url = opencpu_url_fmt('library',  # 'cran',
                              'bcp',
                              'R',
                              'bcp')
        params = {'y': r_list_fmt(df.count_col.tolist())}
        r = unorthodox_request_with_retries([url, params])

        if not r.ok:
            result_dict = {'error': r.text}

        else:
            r_json = dictified_json(r.json(), fix_nans=['posterior.prob'])
            result_dict = {'df': pd.DataFrame({'date_col': df.date_col.tolist(),
                                               'posterior_prob':
                                                   r_json['posterior.prob']})}

        self.write_result_to_hdf5(self.time_col, result_dict)
