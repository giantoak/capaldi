opencpu_url = 'https://54326a7f.ngrok.io/ocpu'
opencpu_root = opencpu_url[:-5]

MAX_RETRIES = 5


def request_with_retries(arg_list, request_type='post'):
    if request_type == 'post':
        from requests import post as req
    elif request_type == 'get':
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