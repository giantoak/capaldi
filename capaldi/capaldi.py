from collections import defaultdict
import os
import pandas as pd
import shutil
import sys
from tempfile import NamedTemporaryFile
from tempfile import mkdtemp
from tqdm import tqdm

from algs import base_bcp
from algs import giant_oak_mmpp
from algs import giant_oak_arima
# from .algs import google_causal_impact
# from .algs import twitter_anomaly_detection
from algs import twitter_breakout

from checks import too_few_buckets
from checks import too_many_empties
from checks import isnt_poisson


error_str_dict = {
    "type": "Capaldi requires an object that can be converted "
    "to a two-column data frame.",
    "date_col": "The left column must be dates or times.",
    "count_col": "The right column must be integers or floats."
}

all_xtab_algs = {'mmpp': giant_oak_mmpp.alg}
all_time_period_algs = {'arima': giant_oak_arima.alg,
                    'bcp': base_bcp.alg,
                    'twitter_breakout': twitter_breakout.alg}

def capaldi(df, algorithms_to_run):
    """
    :param pandas.DataFrame df:
    :param str|list algorithms: list of algorithms
    :returns: `dict` --
    """

    # make scratch directory
    working_dir = mkdtemp()

    time_pairs = [('hhour', 'wday'),
                  ('hour', 'wday'),
                  ('wday', 'mday'),
                  ('wday', 'yweek'),
                  ('wday', 'mweek'),
                  ('wday', 'month'),
                  ('mday', 'month'),
                  ('mweek', 'month'),
                  ('month', 'year')]

    time_periods = ['30min', '1H',
                    '6H', '12H',
                    '1D', '1W', '2W',
                    '1M', '2M', '3M', '6M']

    time_map = {
        'hhour': 48,
        'hour': 24,
        'wday': 7,
        'yweek': 53,
        'mweek': 5,
        'month': 12,
    }

    time_period_algs = {alg: all_time_period_algs[alg]
                        for alg in all_time_period_algs
                        if alg in algorithms_to_run}
    xtab_algs = {alg: all_xtab_algs[alg]
                 for alg in all_xtab_algs
                 if alg in algorithms_to_run}

    # Verify that we can run on the current data frame

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

    try:
        df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])
    except:
        raise TypeError(error_str_dict['date_col'])

    if df.iloc[:, 1].dtype not in ['int64', 'float64']:
        raise TypeError(error_str_dict['count_col'])

    df.columns = ['date_col', 'count_col']
    df = df.sort_values('date_col')

    # Set up crosstabs if we need them.
    # Write to a temporary file

    if len(xtab_algs) > 0:

        df['hhour'] = df.date_col.apply(lambda x: x.hour*2 + int(x.minute*2./60))
        df['hour'] = df.date_col.apply(lambda x: x.hour)
        df['wday'] = df.date_col.apply(lambda x: x.weekday())
        df['mday'] = df.date_col.apply(lambda x: x.day)
        df['yweek'] = df.date_col.apply(lambda x: x.week)
        df['mweek'] = df.date_col.apply(lambda x: int(x.day/7)+1)
        df['month'] = df.date_col.apply(lambda x: x.month)
        df['year'] = df.date_col.apply(lambda x: x.year)

        for col in time_map:
            df[col] = df[col].astype('category',
                                     categories=list(range(time_map[col])),
                                     ordered=True)

        df.year = df.year.astype('category',
                                 categories=list(range(df.year.min(), df.year.max()+1)),
                                 ordered=True)

        xtab_fpath = os.path.join(working_dir, 'base_xtab_file.csv')
        df.to_csv(xtab_fpath, index=False)
        df = df.loc[:, ['date_col', 'count_col']]

    result_dict = dict()
    for algorithm in algorithms_to_run:
        result_dict[algorithm] = defaultdict(dict)

    for time_pair in tqdm(time_pairs, desc='Getting time crosstabs'):

        t_p_key = '{}_{}'.format(time_pair[0], time_pair[1])

        if len(xtab_algs) > 0:
            xtab = pd.pivot_table(pd.read_csv(xtab_fpath,
                                              usecols=['count_col',
                                                       time_pair[0],
                                                       time_pair[1]]),
                                  'count_col', time_pair[0], time_pair[1],
                                  aggfunc=sum).fillna(0)

        for algorithm in tqdm(xtab_algs, desc='Analyzing', leave=False):
            if algorithm == 'mmpp':

                if 'wday' not in time_pair:
                    result_dict['mmpp'][t_p_key]['error'] = 'No wday'
                    continue

                if isnt_poisson.check(xtab):
                    result_dict['mmpp'][t_p_key]['p_warning'] =\
                        ['Poisson Distribution rejected.']

                if 'wday' != time_pair[1]:
                    result_dict['mmpp']['wday_warning'] = 'swapping wday'
                    result_dict['mmpp'][t_p_key]['result'] = \
                        xtab_algs[algorithm](xtab)
                else:
                    result_dict['mmpp'][t_p_key]['result'] = \
                        xtab_algs[algorithm](xtab.T)

            else:
                result_dict[algorithm][t_p_key]['result'] = \
                    xtab_algs[algorithm](xtab.T)

    for time_period in tqdm(time_periods, desc='Grouping by time interval'):

        count_df = df.set_index('date_col').groupby(
            pd.TimeGrouper(time_period)).sum().fillna(0).reset_index()

        if too_few_buckets.check(count_df):
            for algorithm in time_period_algs:
                result_dict[algorithm][time_period]['bucket_warning'] = \
                    'Not enough buckets'
            # If there aren't enough buckets now,
            # there won't be enough buckets with less granularity.
            # So end
            break

        if too_many_empties.check(count_df):
            for algorithm in time_period_algs:
                result_dict[algorithm][time_period]['empty_warning'] = \
                    'Too many empty buckets'

        for algorithm in tqdm(time_period_algs, desc='Analyzing', leave=False):
            result_dict[algorithm][time_period]['result'] = \
                time_period_algs[algorithm](count_df)

    # clean up temporary dir
    shutil.rmtree(working_dir)

    return result_dict


def main(in_fpath, out_fpath, algs):
    import pickle
    df = pd.read_csv(in_fpath)
    if algs == ['all']:
        algs = list(all_xtab_algs) + list(all_time_period_algs)
    result = capaldi(df, algs)
    with open(out_fpath, 'wb') as outfile:
        pickle.dump(result, outfile)


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        main(sys.argv[1], sys.argv[2], sys.argv[3:])
    else:
        print('Usage: python capaldi.py <dataframe_csv> <outfile_path> <alg> [<alg 2> <alg 3>...]')
