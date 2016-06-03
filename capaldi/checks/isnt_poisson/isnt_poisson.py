def check(xtab):
    import numpy as np
    import numpy.random as npr
    from scipy.stats import chisquare
    from ..check_values import POISSON_P_CUTOFF

    xtab_vals = np.ravel(xtab)
    sim_vals = npr.poisson(np.mean(xtab_vals), len(xtab_vals))
    p_val = chisquare(xtab_vals, sim_vals).pvalue

    if p_val < POISSON_P_CUTOFF:
        return True

        return {'p_warning': 'Null hypothesis rejected: {} < {}'.format(p_val, p_cutoff)}
    return dict()


    return False
