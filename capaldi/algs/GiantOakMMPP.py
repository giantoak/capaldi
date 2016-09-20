import h5py
import luigi
import numpy as np
import os
import pandas as pd

from ..XTabDataFrameHDF import XTabDataFrameHDF

from .opencpu_support import r_array_fmt
from .opencpu_support import opencpu_url_fmt
from .opencpu_support import request_with_retries


class GiantOakMMPP(luigi.Task):
    """
    :param pandas.DataFrame xtab:
    :returns: `dict` --
    """
    working_dir = luigi.Parameter()
    time_col_one = luigi.Parameter()
    time_col_two = luigi.Parameter()
    count_col = luigi.Parameter(default='count_col')
    hdf_out_name = luigi.Parameter(default='results.h5')
    hdf_out_key = luigi.Parameter(default='mmpp')

    def requires(self):
        return XTabDataFrameHDF(self.working_dir)

    def run(self):
        xtab = pd.pivot_table(pd.read_csv(self.requires(),
                                          usecols=[self.count_col,
                                                   self.time_col_one,
                                                   self.time_col_two]),
                              self.count_col,
                              self.time_col_one,
                              self.time_col_two,
                              aggfunc=sum).fillna(0)

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
            self.write_result({'error': r.text})

        r_json = r.json()
        self.write_result(
            {key: pd.DataFrame(np.array(r_json[key]).reshape(xtab.shape),
                               index=xtab.index,
                               columns=xtab.columns)
             for key in r_json})

    def write_result(self, dict_to_write):
        """
        Helper func to smooth insertion into hdf5
        :return:
        """
        with h5py.File(self.output()) as outfile:
            outfile['{}/{}_{}'.format(self.hdf_out_key,
                                      self.time_col_one,
                                      self.time_col_two)] = dict_to_write

    def output(self):
        return luigi.LocalTarget(os.path.join(self.working_dir,
                                              self.hdf_out_name))
