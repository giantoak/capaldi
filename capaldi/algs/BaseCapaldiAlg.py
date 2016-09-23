import h5py
import luigi
import os


class BaseCapaldiAlg(luigi.Task):
    """
    vanilla algorithm wrapper
    """
    working_dir = luigi.Parameter()
    time_col = luigi.Parameter()
    hdf_out_name = luigi.Parameter(default='results.h5')
    hdf_out_key = luigi.Parameter(default='unset')

    def write_result_to_hdf5(self, sub_key=None, dict_to_write=None):
        """
        Helper func to smooth insertion into hdf5
        """
        if dict_to_write is None:
            dict_to_write = dict()

        if sub_key is None:
            key = self.hdf_out_key
        else:
            key = '{}/{}'.format(self.hdf_out_key, sub_key)

        with h5py.File(self.output().path) as outfile:
            outfile[key] = dict_to_write

    def output(self):
        return luigi.LocalTarget(os.path.join(self.working_dir,
                                              self.hdf_out_name))
