def alg(xtab):
    """
    :param pandas.DataFrame xtab:
    :returns: `dict` --
    """
    from ..opencpu_support import r_array_fmt
    from ..opencpu_support import opencpu_url_fmt
    from ..opencpu_support import request_with_retries
    import numpy as np
    import pandas as pd

    xtab_vals = ','.join([str(x) for x in np.ravel(xtab)])

    data_str = r_array_fmt(xtab_vals,
                           xtab.shape[1],
                           xtab.shape[0])

    url = opencpu_url_fmt('library',  # 'github', 'giantoak',
                          'mmppr',
                          'R',
                          'sensorMMPP',
                          'json')
    params = {'N': data_str}

    r = request_with_retries([url, params])
    if not r.ok:
        return {'error': r.text}
    r_json = r.json()

    return {key:
            pd.DataFrame(np.array(r_json[key]).reshape(xtab.shape),
                         index=xtab.index,
                         columns=xtab.columns)
            for key in r.json()}

