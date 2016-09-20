import h5py
import ujson as json
import luigi
import os
import pandas as pd

from ..CountsPerTPDataFrameHDF import CountsPerTPDataFrameHDF

from ..checks.TooFewBuckets import TooFewBuckets
from ..checks.TooManyEmpties import TooManyEmpties

from .opencpu_support import dictified_json
from .opencpu_support import opencpu_url_fmt
from .opencpu_support import request_with_retries
from .opencpu_support import r_ts_fmt


class GiantOakARIMA(luigi.Task):
    """
    Run GO's version of the ARIMA algorithm on the supplied time series data.
    :param pandas.DataFrame df: two-column dataframe: date_col, count_col
    :returns: `dict` --
    """
    working_dir = luigi.Parameter()
    time_col = luigi.Parameter()
    count_col = luigi.Parameter(default='count_col')
    hdf_out_name = luigi.Parameter(default='results.h5')
    hdf_out_key = luigi.Parameter(default='arima')

    def requires(self):
        cur_df = CountsPerTPDataFrameHDF(self.working_dir, self.time_col)

        return {'file': cur_df,
                'error_checks': {
                    'too_few_buckets': TooFewBuckets(self.working_dir, cur_df, self.time_col),
                    'too_many_empties': TooManyEmpties(self.working_dir, cur_df, self.time_col)
                  }
                }

    def run(self):
        for error_key in self.requires()['error_checks']:
            jsn = json.loads(open(self.requires()[error_key]).read())
            if not jsn['result']:
                self.write_result(jsn)
                return

        df = pd.read_hdf(self.requires()['file'])

        url = opencpu_url_fmt('library',  # 'github', 'giantoak',
                              'goarima',
                              'R',
                              'arima_all',
                              'json')
        params = {'x': r_ts_fmt(df.count_col.tolist())}

        r = request_with_retries([url, params])
        if not r.ok:
            return {'error': r.text}

        r_json = dictified_json(r.json(),
                                col_to_df_map={'df': ['ub', 'lb', 'resid']})

        r_json['p_value'] = r_json['p.value']
        del r_json['p.value']

        r_json['df']['dates'] = df.date_col

        return r_json

    def write_result(self, dict_to_write):
        """
        Helper func to smooth insertion into hdf5
        :return:
        """
        with h5py.File(self.output()) as outfile:
            outfile['{}/{}'.format(self.hdf_out_key, self.time_col)] =\
                dict_to_write

    def output(self):
        return luigi.LocalTarget(os.path.join(self.working_dir,
                                              self.hdf_out_name))