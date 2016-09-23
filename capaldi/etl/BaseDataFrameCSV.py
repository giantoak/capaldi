import luigi
import os
import pandas as pd

from .OriginalDataFrameCSV import OriginalDataFrameCSV

error_str_dict = {
    "type": "Capaldi requires an object that can be converted "
    "to a two-column data frame.",
    "date_col": "The left column must be dates or times.",
    "count_col": "The right column must be integers or floats."
}


class BaseDataFrameCSV(luigi.ExternalTask):
    in_fpath = luigi.Parameter()
    working_dir = luigi.Parameter()
    out_fname = luigi.Parameter(default='base_df.csv')

    def requires(self):
        return OriginalDataFrameCSV(self.in_fpath)

    def run(self):
        with self.input().open('r') as infile:
            df = pd.read_csv(infile)

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

        df.columns = ['date_col', 'count_col']

        try:
            df.date_col = pd.to_datetime(df.date_col)
        except:
            raise TypeError(error_str_dict['date_col'])

        if df.count_col.dtype not in ['int64', 'float64']:
            raise TypeError(error_str_dict['count_col'])

        df.columns = ['date_col', 'count_col']
        df = df.sort_values('date_col')

        with self.output().open('w') as outfile:
            df.to_csv(outfile, index=False)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.working_dir, self.out_fname))
