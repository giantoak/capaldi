import requests

from formatters import r_array_fmt
from formatters import r_list_fmt
from formatters import r_ts_fmt
from formatters import url_fmt


opencpu_url = 'https://54326a7f.ngrok.io/ocpu'
# opencpu_url = 'https://public.opencpu.org/ocpu'
# opencpu_url = 'http://localhost:5571/ocpu'
opencpu_root = opencpu_url[:-5]


MAX_RETRIES = 5


def request_with_retries(request_type, arg_list):
    for i in range(MAX_RETRIES):
        r = request_type(*arg_list)
        if r.ok:
            return r
    return r


def get_time_series(dates, values):
    """

    :param dates:
    :param values:
    :returns: `dict` - Dictionary of time series values from STL
    """
    # need to post data as a time series object to stl
    url = url_fmt(opencpu_url,
                  'library',
                  'stats',
                  'R',
                  'stl')
    params = {'x': r_ts_fmt(values, 12),
              's.window': 4}
    r = requests.post(url, params)

    # stl returns an object of class stl with components
    #  time.series: a multiple time series with columns seasonal, trend and remainder.
    #  weights: the final robust weights (all one if fitting is not done robustly).
    # #$call	the matched call ... etc
    #  This object is not JSON-Serializable!
    #  We need to do another opencpu call to extract the time.series object

    # gets the tmp storage address of the R object from the first request
    result = r.text.split('\n')[0]
    url2 = url_fmt(opencpu_url,
                   'library',
                   'base',
                   'R',
                   'get',
                   'json')
    # using get to extract the time.series object
    params2 = {'x': '"time.series"',
               'pos': result[10:21]}

    r = request_with_retries(requests.post, [url2, params2])
    if not r.ok:
        return {'error': r.text}

    r_json = r.json()

    return {
        'seasonal':
        [{'date': d, 'value': v} for d, v in zip(dates,
                                                 [x[0] for x in r_json])],
        'trend':
        [{'date': d, 'value': v} for d, v in zip(dates,
                                                 [x[1] for x in r_json])],
        'remainder':
        [{'date': d, 'value': v} for d, v in zip(dates,
                                                 [x[2] for x in r_json])]
    }


def base_bcp(df):
    """

    param pandas.DataFrame df: two-column dataframe: date_col, count_col
    :returns: `dict` --
    """
    from numpy import nan
    from pandas import DataFrame

    url = url_fmt(opencpu_url,
                  'library',  # 'cran',
                  'bcp',
                  'R',
                  'bcp')
    params = {'y': r_list_fmt(df.count_col.tolist())}
    r = request_with_retries(requests.post, [url, params])
    if not r.ok:
        return {'error_1': r.text}

    resource_suffix = r.text.split('\n')[0]
    # val_loc = resource_url[10:21]

    # url2 = url_fmt(opencpu_url,
    #                'library',
    #                'base',
    #                'R',
    #                'get',
    #                'json')
    # params2 = {'x': '"posterior.prob"',
    #            'pos': val_loc}

    # r = request_with_retries(requests.post, [url2, params2])
    # url_2 = '{}{}/json?force=true'.format(opencpu_root, resource_suffix)
    url_2 = r.headers['Location']+'R/.val/json?force=true'
    r = request_with_retries(requests.get, [url_2])
    if not r.ok:
        return {'error_2': r.text}

    r_json = r.json()
    return DataFrame({'date_col': df.date_col.tolist(),
                      'posterior_prob': [x if x not in ['NA', 'NAN'] else nan
                                         for x in r_json['posterior.prob']]})


def giant_oak_mmpp(xtab):
    """
    :param pandas.DataFrame xtab:
    :returns: `dict` --
    """
    from numpy import ravel
    import requests

    xtab_vals = ','.join([str(x) for x in ravel(xtab)])

    data_str = r_array_fmt(xtab_vals,
                           xtab.shape[1],
                           xtab.shape[0])

    url = url_fmt(opencpu_url,

                  'library',  # 'github', 'giantoak',
                  'mmppr',
                  'R',
                  'sensorMMPP',
                  'json')
    params = {'N': data_str}

    r = request_with_retries(requests.post, [url, params])
    if not r.ok:
        return {'error': r.text}
    r_json = r.json()

    import numpy as np
    import pandas as pd

    # return {
    #     'L': pd.DataFrame(np.array(r_json['L'])),
    #     'Z': pd.DataFrame(np.array(r_json['Z']))
    # }

    return {key:
            pd.DataFrame(np.array(r.json()[key]).reshape(xtab.shape),
                         index=xtab.index,
                         columns=xtab.columns)
            for key in r.json()}


def giant_oak_arima(df):
    """
    Run GO's version of the ARIMA algorithm on the supplied time series data.
    :param pandas.DataFrame df: two-column dataframe: date_col, count_col
    :returns: `dict` --
    """
    from pandas import DataFrame
    import requests

    url = url_fmt(opencpu_url,
                  'library',  # 'github', 'giantoak',
                  'goarima',
                  'R',
                  'arima_all',
                  'json')
    params = {'x': r_ts_fmt(df.count_col.tolist())}

    r = request_with_retries(requests.post, [url, params])
    if not r.ok:
        return {'error': r.text}

    r_json = r.json()

    r_json['p_value'] = r_json['p.value'][0]
    del r_json['p.value']

    for key in ['D', 'theta', 'phi']:
        if len(r_json[key]) == 0:
            r_json[key] = None
        elif len(r_json[key]) == 1:
            r_json[key] = r_json[key][0]

    r_json['df'] = DataFrame({'dates': df.date_col,
                              'ub': r_json['ub'],
                              'lb': r_json['lb'],
                              'resid': r_json['resid']})
    del r_json['ub']
    del r_json['lb']
    del r_json['resid']

    return r_json


def google_causal_impact(dates, values, bp):
    """

    :param dates:
    :param values:
    :param bp:
    :returns: `str` --
    """
    url = url_fmt(opencpu_url,
                  'library',  # 'github', 'google',
                  'CausalImpact',
                  'R',
                  'CausalImpact',
                  'json')
    length = len(values)
    data2 = r_ts_fmt(values)
    params = {'data': data2,
              'pre.period': 'c(1,{})'.format(bp),
              'post.period': 'c({},{})'.format(bp + 1, length)}

    r = request_with_retries(requests.post, [url, params])
    if not r.ok:
        return {'error': r.text}
    res = r.text.split('\n')[0]

    # url2 = 'https://public.opencpu.org/ocpu/library/base/R/get/'
    # params2 = {'x':'"series"','pos':res[10:21]}
    url2 = '{}{}/json?force=true'.format(opencpu_root, res)
    r2 = request_with_retries(requests.get, [url2])
    if not r2.ok:
        return {'error': r2.text}

    data = r2.json()
    data['date'] = dates
    return {'date': data['date'], 'series': data['series']}


def twitter_anomaly_detection(dates, values):
    """

    :param dates:
    :param values:
    :returns: `str` --
    """
    from collections import OrderedDict
    import simplejson as json

    url = url_fmt(opencpu_url,
                  'library',  # 'github', 'twitter',
                  'AnomalyDetection',
                  'R',
                  'AnomalyDetectionTs',
                  'json')

    data = {'x':
                [OrderedDict([('timestamp', str(d)), ('count', v)])
                 for d, v in zip(dates, values)]
            }

    headers = {'Content-Type': 'application/json'}

    for i in range(MAX_RETRIES):
        r = requests.post(url, json.dumps(data), headers=headers)
        if r.ok:
            return r.json()
    return {'error': r.text}


def twitter_breakout(df):
    """

    :param pandas.DataFrame df: two-column dataframe: date_col, count_col
    :returns: `dict` --
    """
    url = url_fmt(opencpu_url,
                  'library',  # 'github', 'twitter',
                  'BreakoutDetection',
                  'R',
                  'breakout',
                  'json')
    params = {'Z': r_list_fmt(df.count_col.tolist())}

    r = request_with_retries(requests.post, [url, params])
    if not r.ok:
        return {'error': r.text}

    r_json = r.json()
    for key in r_json:
        if len(r_json[key]) == 0:
            r_json[key] = None
        elif len(r_json[key]) == 1:
            r_json[key] = r_json[key][0]

    return r_json
