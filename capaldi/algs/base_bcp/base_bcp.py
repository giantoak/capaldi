def alg(df):
    """

    param pandas.DataFrame df: two-column dataframe: date_col, count_col
    :returns: `dict` --
    """
    from ..opencpu_support import opencpu_url_fmt
    from ..opencpu_support import r_list_fmt
    from ..opencpu_support import request_with_retries
    import numpy as np
    import pandas as pd

    url = opencpu_url_fmt('library',  # 'cran',
                          'bcp',
                          'R',
                          'bcp')
    params = {'y': r_list_fmt(df.count_col.tolist())}
    r = request_with_retries([url, params])
    if not r.ok:
        return {'error_1': r.text}

    url_2 = r.headers['Location']+'R/.val/json?force=true'
    r = request_with_retries([url_2], 'get')
    if not r.ok:
        return {'error_2': r.text}

    r_json = r.json()
    return pd.DataFrame({'date_col': df.date_col.tolist(),
                         'posterior_prob': [x if x not in ['NA', 'NAN'] else np.nan
                                            for x in r_json['posterior.prob']]})


def sanity_check():
    return dict()
