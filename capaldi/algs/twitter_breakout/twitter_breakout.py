def alg(df):
    """

    :param pandas.DataFrame df: two-column dataframe: date_col, count_col
    :returns: `dict` --
    """
    from ..opencpu_support import dictified_json
    from ..opencpu_support import opencpu_url_fmt
    from ..opencpu_support import r_list_fmt
    from ..opencpu_support import request_with_retries

    url = opencpu_url_fmt('library',  # 'github', 'twitter',
                          'BreakoutDetection',
                          'R',
                          'breakout',
                          'json')
    params = {'Z': r_list_fmt(df.count_col.tolist())}

    r = request_with_retries([url, params])
    if not r.ok:
        return {'error': r.text}

    return dictified_json(r.json())
