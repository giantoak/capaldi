def alg(dates, values, bp):
    """

    :param dates:
    :param values:
    :param bp:
    :returns: `str` --
    """
    from ..opencpu_support import opencpu_url_fmt
    from ..opencpu_support import r_ts_fmt
    from ..opencpu_support import request_with_retries
    from ..opencpu_support import opencpu_root

    url = opencpu_url_fmt('library',  # 'github', 'google',
                          'CausalImpact',
                          'R',
                          'CausalImpact',
                          'json')
    length = len(values)
    data2 = r_ts_fmt(values)
    params = {'data': data2,
              'pre.period': 'c(1,{})'.format(bp),
              'post.period': 'c({},{})'.format(bp + 1, length)}

    r = request_with_retries([url, params])
    if not r.ok:
        return {'error': r.text}
    res = r.text.split('\n')[0]

    # url2 = 'https://public.opencpu.org/ocpu/library/base/R/get/'
    # params2 = {'x':'"series"','pos':res[10:21]}
    url2 = '{}{}/json?force=true'.format(opencpu_root, res)
    r2 = request_with_retries([url2], 'get')
    if not r2.ok:
        return {'error': r2.text}

    data = r2.json()
    data['date'] = dates
    return {'date': data['date'], 'series': data['series']}


def sanity_check():
    return dict()
