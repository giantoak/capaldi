

def alg(df):
    """
    Run GO's version of the ARIMA algorithm on the supplied time series data.
    :param pandas.DataFrame df: two-column dataframe: date_col, count_col
    :returns: `dict` --
    """
    from ..opencpu_support import opencpu_url_fmt
    from ..opencpu_support import request_with_retries
    from ..opencpu_support import r_ts_fmt
    import pandas as pd

    url = opencpu_url_fmt('library',  # 'github', 'giantoak',
                          'goarima',
                          'R',
                          'arima_all',
                          'json')
    params = {'x': r_ts_fmt(df.count_col.tolist())}

    r = request_with_retries([url, params])
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

    r_json['df'] = pd.DataFrame({'dates': df.date_col,
                                 'ub': r_json['ub'],
                                 'lb': r_json['lb'],
                                 'resid': r_json['resid']})
    del r_json['ub']
    del r_json['lb']
    del r_json['resid']

    return r_json


def sanity_check():
    return dict()