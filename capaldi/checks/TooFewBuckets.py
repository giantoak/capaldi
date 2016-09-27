import luigi

from .BaseCapaldiCheck import BaseCapaldiCheck

from capaldi.checks.check_values import MIN_BUCKETS


class TooFewBuckets(BaseCapaldiCheck):
    label = 'TooFewBuckets'
    output_prefix = luigi.Parameter(default='TooFewBuckets')

    def check(self, df):

        result_dict = dict()

        if df.shape[0] < MIN_BUCKETS:
            result_dict['result'] = True
            result_dict['bucket_warning'] = 'Not enough buckets'
        else:
            result_dict['result'] = False

        return result_dict
