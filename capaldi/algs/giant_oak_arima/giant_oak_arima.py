def alg(df):
    """
    Run GO's version of the ARIMA algorithm on the supplied time series data.
    :param pandas.DataFrame df: two-column dataframe: date_col, count_col
    :returns: `dict` --
    """
    from ..opencpu_support import dictified_json
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

    r_json = dictified_json(r.json(),
                            col_to_df_map={'df': ['ub', 'lb', 'resid']})

    r_json['p_value'] = r_json['p.value']
    del r_json['p.value']

    r_json['df']['dates'] = df.date_col

    return r_json


def sanity_check():
    return dict()