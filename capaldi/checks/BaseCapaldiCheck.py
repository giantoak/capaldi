import ujson as json
import luigi
import os
import pandas as pd


class BaseCapaldiCheck(luigi.Task):
    label = 'BaseCapaldiCheck'
    working_dir = luigi.Parameter()
    df_to_use = luigi.TaskParameter()
    output_prefix = luigi.Parameter()
    output_suffix = luigi.Parameter()

    def requires(self):
        return self.df_to_use

    def check(self, df):
        """
        Function that WILL BE OVERRIDDEN in sub classes
        Defines the check to run
        :param pandas.DataFrame df:
        :returns: `dict` -- dictionary of check results
        """
        return dict()

    def run(self):

        with self.input().open('r') as infile:
            df = pd.read_csv(infile)

        result_dict = self.check(df)
        with self.output().open('wb') as outfile:
            json.dump(result_dict, outfile)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.working_dir,
                                              '{}_{}.json'.format(
                                                  self.output_prefix,
                                                  self.output_suffix)))
