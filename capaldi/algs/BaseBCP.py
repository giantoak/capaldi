import ujson as json
import pandas as pd

from .BaseCapaldiAlg import BaseCapaldiAlg

import capaldi.etl as etl
import capaldi.checks as checks

from capaldi.algs.opencpu_support import dictified_json
from capaldi.algs.opencpu_support import opencpu_url_fmt
from capaldi.algs.opencpu_support import unorthodox_request_with_retries
from capaldi.algs.opencpu_support import r_list_fmt


class BaseBCP(BaseCapaldiAlg):

    def requires(self):
        cur_df = etl.CountsPerTPDataFrameCSV(working_dir=self.working_dir,
                                             time_period=self.time_col)
        return {'file': cur_df,
                'error_checks': {
                    'too_few_buckets': checks.TooFewBuckets(
                        working_dir=self.working_dir,
                        df_to_use=cur_df,
                        output_suffix=self.time_col),
                    'too_many_empties': checks.TooManyEmpties(
                        working_dir=self.working_dir,
                        df_to_use=cur_df,
                        output_suffix=self.time_col)
                }
                }

    def alg(self, df):
        url = opencpu_url_fmt('library',  # 'cran',
                              'bcp',
                              'R',
                              'bcp')
        params = {'y': r_list_fmt(df.count_col.tolist())}
        r = unorthodox_request_with_retries([url, params])

        if not r.ok:
            return {'error': r.text}

        r_json = dictified_json(r.json(), fix_nans=['posterior.prob'])
        return {'df': json.loads(pd.DataFrame({'date_col':
                                                   df.date_col.tolist(),
                                               'posterior_prob':
                                                   r_json['posterior.prob']
                                               }).to_json())}
