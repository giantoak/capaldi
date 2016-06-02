def alg(df):
    """

    param pandas.DataFrame df: two-column dataframe: date_col, count_col
    :returns: `dict` --
    """
    from ..opencpu_support import opencpu_url_fmt
    from ..opencpu_support import r_list_fmt
    import numpy as np
    from ..opencpu_support import unorthodox_request_with_retries
    import pandas as pd

    url = opencpu_url_fmt('library',  # 'cran',
                          'bcp',
                          'R',
                          'bcp')
    params = {'y': r_list_fmt(df.count_col.tolist())}
    r = unorthodox_request_with_retries([url, params])

    r_json = r.json()
    return pd.DataFrame({'date_col': df.date_col.tolist(),
                         'posterior_prob': [x if x not in ['NA', 'NAN'] else np.nan
                                            for x in r_json['posterior.prob']]})


def sanity_check():
    return dict()
