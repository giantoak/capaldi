def alg(df):
    """

    param pandas.DataFrame df: two-column dataframe: date_col, count_col
    :returns: `pandas.DataFrame` --
    """
    from ..opencpu_support import opencpu_url_fmt
    from ..opencpu_support import r_list_fmt
    from ..opencpu_support import unorthodox_request_with_retries
    from ..opencpu_support import dictified_json
    import pandas as pd

    url = opencpu_url_fmt('library',  # 'cran',
                          'bcp',
                          'R',
                          'bcp')
    params = {'y': r_list_fmt(df.count_col.tolist())}
    r = unorthodox_request_with_retries([url, params])
    r_json = dictified_json(r.json(), fix_nans=['posterior.prob'])

    return pd.DataFrame({'date_col': df.date_col.tolist(),
                         'posterior_prob': r_json['posterior.prob']})
