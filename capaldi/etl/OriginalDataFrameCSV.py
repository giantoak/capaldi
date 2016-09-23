import luigi


class OriginalDataFrameCSV(luigi.ExternalTask):
    dataframe_csv = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.dataframe_csv)
