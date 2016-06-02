opencpu_url = 'https://54326a7f.ngrok.io/ocpu'
opencpu_root = opencpu_url[:-5]

MAX_RETRIES = 5


def request_with_retries(arg_list, request_type='post'):
    """

    :param list arg_list:
    :param str request_type:
    :returns: `` --
    """
    if request_type == 'post':
        from requests import post as req
    else:
        # elif request_type == 'get':
        from requests import get as req

    for i in range(MAX_RETRIES):
        r = req(*arg_list)
        if r.ok:
            return r
    return r


def opencpu_url_fmt(*args):
    return '{}/{}'.format(opencpu_url, url_fmt(args))


def url_fmt(*args):
    """
    Joins given arguments into a url, stripping trailing slashes.
    """
    return '/'.join([str(x).rstrip('/') for x in args])


def r_list_fmt(x):
    """

    :param x: Some variable
    :returns: `str` -- variable formatted as r list
    """
    if isinstance(x, (list, set)):
        return 'c({})'.format(str(x)[1:-1])
    return 'c({})'.format(x)


def r_ts_fmt(x, frequency=None):
    """

    :param x:
    :param frequency:
    :returns: `str` -- variable formatted as r time series
    """
    if frequency is None:
        return 'ts({})'.format(r_list_fmt(x))

    return 'ts({}, frequency={})'.format(r_list_fmt(x), frequency)


def r_array_fmt(x, dim_one, dim_two):
    """

    :param x:
    :param int dim_one:
    :param int dim_two:
    :returns: `str` -- variables formatted as r array
    """
    return 'array({}, dim={})'.format(r_list_fmt(x), r_list_fmt([dim_one, dim_two]))


def get_time_series(dates, values):
    """

    :param dates:
    :param values:
    :returns: `dict` - Dictionary of time series values from STL
    """
    import requests
    # need to post data as a time series object to stl
    url = opencpu_url_fmt('library',
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
    url2 = opencpu_url_fmt('library',
                           'base',
                           'R',
                           'get',
                           'json')
    # using get to extract the time.series object
    params2 = {'x': '"time.series"',
               'pos': result[10:21]}

    r = request_with_retries([url2, params2])
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
