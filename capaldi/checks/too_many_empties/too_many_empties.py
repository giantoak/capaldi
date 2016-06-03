def check(df):
    import numpy as np
    from ..check_values import MAX_EMPTY_PROPORTION

    empty_buckets = np.sum(df.count_col.values < 1)
    if empty_buckets/df.shape[0] <= MAX_EMPTY_PROPORTION:
        return False
    return True
