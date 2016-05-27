from collections import defaultdict
import numpy as np
import numpy.random as npr
import pandas as pd
from scipy.stats import chisquare
from tqdm import tqdm

from opencpu_tools import base_bcp
from opencpu_tools import giant_oak_arima
from opencpu_tools import giant_oak_mmpp
# from opencpu_tools import twitter_anomaly_detection
from opencpu_tools import twitter_breakout

error_str_dict = {
    "type": "Capyldi requires an object that can be converted "
    "to a two-column data frame.",
    "date_col": "The left column must be dates or times.",
    "count_col": "The right column must be integers or floats."
}

P_VAL = 0.05
MIN_BUCKETS = 10
MAX_EMPTY_BUCKETS = 30


def _mmpp_sanity_check(xtab):
    xtab_vals = np.ravel(xtab)
    sim_vals = npr.poisson(np.mean(xtab_vals), len(xtab_vals))
    p_val = chisquare(xtab_vals, sim_vals).pvalue

    if p_val < P_VAL:
        return {'p_warning':
                    'Null hypothesis rejected: {} < {}'.format(p_val, P_VAL)}
    return dict()


def capyldi(df, algorithms='all'):
    """
    :param pandas.DataFrame df:
    :param str|list algorithms:
    :returns: `dict` --
    """

    xtab_algs = {'mmpp': giant_oak_mmpp}
    time_period_algs = {'arima': giant_oak_arima,
                        'bcp': base_bcp,
                        'twitter_breakout': twitter_breakout}

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

    if algorithms == 'all':
        algorithms = list(xtab_algs) + list(time_period_algs)
    elif isinstance(algorithms, str):
        algorithms = [algorithms]

    time_period_algs = {alg: time_period_algs[alg]
                        for alg in time_period_algs
                        if alg in algorithms}
    xtab_algs = {alg: xtab_algs[alg]
                 for alg in xtab_algs
                 if alg in algorithms}

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

    time_map = {
        'hhour': 48,
        'hour': 24,
        'wday': 7,
        'yweek': 53,
        'mweek': 5,
        'month': 12,
    }

    df = df.sort_values('date_col')

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

    result_dict = dict()
    for algorithm in algorithms:
        result_dict[algorithm] = defaultdict(dict)

    for time_pair in tqdm(time_pairs, desc='Getting time crosstabs'):

        t_p_key = '{}_{}'.format(time_pair[0], time_pair[1])

        if len(xtab_algs) > 0:
            xtab = pd.pivot_table(df, 'count_col', time_pair[0], time_pair[1],
                                  aggfunc=sum).fillna(0)

        for algorithm in tqdm(xtab_algs, desc='Analyzing', leave=False):
            if algorithm == 'mmpp':

                if 'wday' not in time_pair:
                    result_dict['mmpp'][t_p_key]['error'] = 'No wday'
                    continue

                warn_dict = _mmpp_sanity_check(xtab)
                for key in warn_dict:
                    result_dict['mmpp'][t_p_key][key] = warn_dict[key]

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

    if len(xtab_algs) > 0:
        df = df.loc[:, ['date_col', 'count_col']]

    for time_period in tqdm(time_periods, desc='Grouping by time interval'):

        count_df = df.set_index('date_col').groupby(
            pd.TimeGrouper(time_period)).sum().fillna(0).reset_index()

        if count_df.count_col.shape[0] < MIN_BUCKETS:
            for algorithm in time_period_algs:
                result_dict[algorithm][time_period]['bucket_warning'] = \
                    'Not enough buckets: {} < {}'.format(count_df.count_col.shape[0],
                                                         MIN_BUCKETS)

            # If there aren't enough buckets now,
            # there won't be enough buckets with less granularity.
            # So end
            break

        empty_buckets = np.sum(count_df.count_col.values < 1)
        if empty_buckets > MAX_EMPTY_BUCKETS:
            for algorithm in time_period_algs:
                result_dict[algorithm][time_period]['empty_warning'] = \
                    'Too many empty buckets: {} > {}'.format(empty_buckets,
                                                             MAX_EMPTY_BUCKETS)

        for algorithm in tqdm(time_period_algs, desc='Analyzing', leave=False):
            result_dict[algorithm][time_period]['result'] = \
                time_period_algs[algorithm](count_df)

    return result_dict
