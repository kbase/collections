"""
Methods in this script are designed to compute a genome feature.

Methods should return a series containing only the computed genome feature. It will then be appended to the result frame.
"""


# TODO: prototype method should remove
def high_checkm_marker_count(df):
    sr = df['checkm_marker_count'] > 150
    sr.rename('high_checkm_marker_count', inplace=True)
    return sr
