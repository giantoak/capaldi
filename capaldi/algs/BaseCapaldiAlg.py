import h5py
import luigi
import os
import pandas as pd
import ujson as json


class BaseCapaldiAlg(luigi.Task):
    """
    vanilla algorithm wrapper
    """
    working_dir = luigi.Parameter()
    time_col = luigi.Parameter()
    json_out_fname = luigi.Parameter()
    wrapper_keys = luigi.ListParameter(default=[])
    count_col = luigi.Parameter(default='count_col')

    def load_dataframe(self):
        """
        Function that loads dataframe for use by algorithm.
        Assumes that we can just load the data as-is,
        Override as needed (e.g. GiantOakMMPP)
        :returns: `pandas.DataFrame` -- DataFrame for alg
        """
        with self.input()['file'].open('r') as infile:
            df = pd.read_csv(infile, parse_dates=['date_col'])

        return df

    def alg(self, df):
        """
        Function that WILL BE OVERRIDDEN in sub classes
        Defines the TS Algorithm
        :param pandas.DataFrame df:
        :returns: `dict` -- dictionary of algorithm results
        """
        return dict()

    def run(self):
        for key in self.input()['error_checks']:
            with self.input()['error_checks'][key].open('r') as infile:
                jsn = json.load(infile)
                if not jsn['result']:
                    self.write_results_to_json({key: jsn})
                    return

        df = self.load_dataframe()
        result_dict = self.alg(df)
        self.write_results_to_json(result_dict)

    def write_results_to_json(self, dict_to_write):
        for key in self.wrapper_keys:
            dict_to_write = {key: dict_to_write}

        with self.output().open('wb') as outfile:
            json.dump(dict_to_write, outfile)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.working_dir,
                                              self.json_out_fname))
