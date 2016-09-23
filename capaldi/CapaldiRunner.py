import luigi
import os
from tempfile import mkdtemp
import shutil

from config import time_pairs
from config import time_periods

from etl import BaseDataFrameCSV
from algs import GiantOakMMPP
from algs import BaseBCP
from algs import TwitterBreakout
from algs import GiantOakARIMA


xtab_algs = {'mmpp': GiantOakMMPP}

count_algs = {'arima': GiantOakARIMA,
              'bcp': BaseBCP,
              'twitter_breakout': TwitterBreakout}


class DummyLocalTask(luigi.ExternalTask):
    dummy_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.dummy_path)


class MoveResultsToFinal(luigi.Task):

    working_dir = luigi.Parameter()
    in_fname = luigi.Parameter()
    out_fpath = luigi.Parameter()

    def requires(self):
        return DummyLocalTask(self.working_dir),\
               DummyLocalTask(os.path.join(self.working_dir,
                                           self.in_fname))

    def run(self):
        with self.input()[1].open('r') as infile:
            with self.output().open('w') as outfile:
                outfile.write(infile.read())

        shutil.rmtree(self.input()[0].path)

    def output(self):
        return luigi.LocalTarget(self.out_fpath)


class CapaldiRunner(luigi.WrapperTask):

    in_fpath = luigi.Parameter()
    out_fpath = luigi.Parameter()
    algs_to_run = luigi.ListParameter(default=['all'])

    def requires(self):

        print(self.in_fpath)
        print(self.out_fpath)
        print(self.algs_to_run)

        if len(self.algs_to_run) == 0:
            return

        if 'all' in self.algs_to_run:
            self.algs_to_run = list(xtab_algs) + list(count_algs)

        working_dir = mkdtemp()
        yield BaseDataFrameCSV(in_fpath=self.in_fpath,
                               working_dir=working_dir)

        for alg in self.algs_to_run:
            if alg in xtab_algs:
                for t_p in time_pairs:
                    yield xtab_algs[alg](working_dir=working_dir,
                                         time_col=t_p[0],
                                         time_col_two=t_p[1])
            elif alg in count_algs:
                for time_period in time_periods:
                    yield count_algs[alg](working_dir=working_dir,
                                          time_col=time_period)
            else:
                # Unknown time series algorithm
                pass

        # TODO get these dependencies correct
        yield MoveResultsToFinal(working_dir,
                                 'base_df.csv',  # 'results.csv',
                                 self.out_fpath)
