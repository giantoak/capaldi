time_pairs = [('hhour', 'wday'),
              ('hour', 'wday'),
              ('wday', 'mday'),
              ('wday', 'yweek'),
              ('wday', 'mweek'),
              ('wday', 'month'),
              ('mday', 'month'),
              ('mweek', 'month'),
              ('month', 'year')]

time_periods = ['30min', '1H',
                '6H', '12H',
                '1D', '1W', '2W',
                '1M', '2M', '3M', '6M']

time_map = {
    'hhour': 48,
    'hour': 24,
    'wday': 7,
    'yweek': 53,
    'mweek': 5,
    'month': 12,
}
