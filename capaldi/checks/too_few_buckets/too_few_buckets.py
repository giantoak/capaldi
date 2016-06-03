def check(df):
    from ..check_values import MIN_BUCKETS

    if df.shape[0] >= MIN_BUCKETS:
        return False

    return True
