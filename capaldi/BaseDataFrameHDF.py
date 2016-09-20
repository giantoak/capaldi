import luigi
import os
import pandas as pd

from .BaseDataFrameCSV import BaseDataFrameCSV

error_str_dict = {
    "type": "Capaldi requires an object that can be converted "
    "to a two-column data frame.",
    "date_col": "The left column must be dates or times.",
    "count_col": "The right column must be integers or floats."
}


class BaseDataFrameHDF(luigi.ExternalTask):
    dataframe_csv = luigi.Parameter()
    working_dir = luigi.Parameter()
    hdf_out_name = luigi.Parameter(default='base_df.h5')
    hdf_out_key = luigi.Parameter(default='base_df')

    def requires(self):
        return BaseDataFrameCSV(self.dataframe_csv)

    def run(self):

        df = pd.read_csv(self.requires())

        if not isinstance(df, pd.DataFrame):
            if isinstance(pd.Series, df):
                df = df.reset_index()
            else:
                try:
                    df = pd.DataFrame(df)
                except:
                    raise TypeError(' '.join([error_str_dict['type'],
                                              error_str_dict['date_col'],
                                              error_str_dict['count_col']]))

        try:
            df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])
        except:
            raise TypeError(error_str_dict['date_col'])

        if df.iloc[:, 1].dtype not in ['int64', 'float64']:
            raise TypeError(error_str_dict['count_col'])

        df.columns = ['date_col', 'count_col']
        df = df.sort_values('date_col')

        df.to_hdf(self.output(), self.hdf_out_key)

    def output(self):
        return os.path.join(self.working_dir, self.hdf_name)
