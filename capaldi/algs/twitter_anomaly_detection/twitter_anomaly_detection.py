def alg(dates, values):
    """

    :param dates:
    :param values:
    :returns: `str` --
    """
    from collections import OrderedDict
    import simplejson as json
    from ..opencpu_support import opencpu_url_fmt
    from ..opencpu_support import MAX_RETRIES
    import requests

    url = opencpu_url_fmt('library',  # 'github', 'twitter',
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


def sanity_check():
    return dict()
