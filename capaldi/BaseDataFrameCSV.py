import luigi


class BaseDataFrameCSV(luigi.ExternalTask):
    dataframe_csv = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.dataframe_csv)
