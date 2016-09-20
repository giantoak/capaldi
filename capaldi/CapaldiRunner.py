import luigi

from .config import time_pairs
from .config import time_periods

from .BaseDataFrameHDF import BaseDataFrameHDF
from .algs.GiantOakMMPP import GiantOakMMPP
from .algs.GiantOakARIMA import GiantOakARIMA

class CapaldiRunner(luigi.WrapperTask):

    dataframe_csv = luigi.Parameter()
    working_dir = luigi.Parameter()
    algs_to_run = luigi.ListParameter(default=[])

    def requires(self):

        if len(self.algs_to_run) == 0:
            return

        yield BaseDataFrameHDF(self.dataframe_csv, self.working_dir)

        if 'mmpp' in self.algs_to_run:
            for t_p in time_pairs:
                if 'wday' not in t_p:
                    continue

                yield GiantOakMMPP(self.working_dir, t_p[0], t_p[1])


        for time_period in time_periods:
            for alg in self.algs_to_run
                if alg == 'arima':
                    yield GiantOakARIMA(self.working_dir, time_period)

