import luigi

from .BaseCapaldiAlg import BaseCapaldiAlg

import capaldi.etl as etl
import capaldi.checks as checks

from capaldi.algs.opencpu_support import dictified_json
from capaldi.algs.opencpu_support import opencpu_url_fmt
from capaldi.algs.opencpu_support import r_list_fmt
from capaldi.algs.opencpu_support import request_with_retries


class TwitterBreakout(BaseCapaldiAlg):
    time_col = luigi.Parameter()
    hdf_out_key = luigi.Parameter(default='twitter_breakout')

    def requires(self):
        cur_df = etl.CountsPerTPDataFrameCSV(working_dir=self.working_dir,
                                             time_col=self.time_col)

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

    def alg(self, df):
        url = opencpu_url_fmt('library',  # 'github', 'twitter',
                              'BreakoutDetection',
                              'R',
                              'breakout',
                              'json')
        params = {'Z': r_list_fmt(df.count_col.tolist())}

        r = request_with_retries([url, params])

        if not r.ok:
            return {'error': r.text}

        return dictified_json(r.json())
