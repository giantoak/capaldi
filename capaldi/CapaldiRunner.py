import luigi
import shutil
from tempfile import mkdtemp

from .config import time_pairs

from .BaseDataFrameHDF import BaseDataFrameHDF
from .algs.GiantOakMMPP import GiantOakMMPP


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
                yield GiantOakMMPP(self.working_dir, t_p[0], t_p[1])
