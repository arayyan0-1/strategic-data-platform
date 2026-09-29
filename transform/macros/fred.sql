{# The FRED series of core.rates, each with its column name. #}
{% macro fred_columns() %}
    {{ return({
        'DTB4WK': 'bill_4w', 'DTB3': 'bill_3m',
        'DGS1MO': 't_1m', 'DGS3MO': 't_3m', 'DGS6MO': 't_6m', 'DGS1': 't_1y', 'DGS2': 't_2y',
        'DGS3': 't_3y', 'DGS5': 't_5y', 'DGS7': 't_7y', 'DGS10': 't_10y', 'DGS20': 't_20y',
        'DGS30': 't_30y', 'DFII5': 'real_5y', 'DFII7': 'real_7y', 'DFII10': 'real_10y',
        'DFII20': 'real_20y', 'DFII30': 'real_30y',
        'T5YIE': 'breakeven_5y', 'T10YIE': 'breakeven_10y', 'T5YIFR': 'breakeven_5y5y',
        'DFF': 'fed_funds', 'SOFR': 'sofr',
        'BAMLH0A0HYM2': 'hy_oas', 'BAMLC0A0CM': 'ig_oas',
        'VIXCLS': 'vix', 'VXVCLS': 'vix_3m', 'DTWEXBGS': 'dollar',
        'NFCI': 'nfci', 'STLFSI4': 'stlfsi'}) }}
{% endmacro %}
