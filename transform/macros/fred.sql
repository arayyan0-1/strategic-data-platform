{# The FRED series of core.rates, each with its column name. #}
{% macro fred_columns() %}
    {{ return({
        'DTB4WK': 'bill_4w', 'DTB3': 'bill_3m',
        'DGS1MO': 't_1m', 'DGS3MO': 't_3m', 'DGS6MO': 't_6m', 'DGS1': 't_1y', 'DGS2': 't_2y',
        'DGS3': 't_3y', 'DGS5': 't_5y', 'DGS7': 't_7y', 'DGS10': 't_10y', 'DGS20': 't_20y',
        'DGS30': 't_30y', 'DFII5': 'real_5y', 'DFII7': 'real_7y', 'DFII10': 'real_10y',
        'DFII20': 'real_20y', 'DFII30': 'real_30y',
        'T5YIE': 'breakeven_5y', 'T10YIE': 'breakeven_10y', 'T5YIFR': 'breakeven_5y5y',
        'THREEFYTP10': 'term_premium_10y',
        'DFF': 'fed_funds', 'SOFR': 'sofr',
        'BAMLH0A0HYM2': 'hy_oas', 'BAMLC0A0CM': 'ig_oas',
        'BAMLC0A1CAAA': 'oas_aaa', 'BAMLC0A2CAA': 'oas_aa', 'BAMLC0A3CA': 'oas_a',
        'BAMLC0A4CBBB': 'oas_bbb', 'BAMLH0A1HYBB': 'oas_bb', 'BAMLH0A2HYB': 'oas_b',
        'BAMLH0A3HYC': 'oas_ccc',
        'IORB': 'iorb', 'RRPONTSYAWARD': 'rrp_rate', 'EFFR': 'effr', 'OBFR': 'obfr',
        'TGCRRATE': 'tgcr', 'SOFR1': 'sofr_p1', 'SOFR25': 'sofr_p25', 'SOFR75': 'sofr_p75',
        'SOFR99': 'sofr_p99', 'SOFRVOL': 'sofr_volume', 'DCPF3M': 'cp_fin_3m',
        'DCPN3M': 'cp_nonfin_3m', 'DPCREDIT': 'discount_rate',
        'WALCL': 'fed_assets', 'WSHOTSL': 'fed_treasuries', 'WSHOMCB': 'fed_mbs',
        'WRESBAL': 'reserves', 'WTREGEN': 'tga', 'RRPONTSYD': 'rrp', 'RPONTSYD': 'standing_repo',
        'VIXCLS': 'vix', 'VXVCLS': 'vix_3m', 'DTWEXBGS': 'dollar',
        'NFCI': 'nfci', 'STLFSI4': 'stlfsi',
        'DEXUSEU': 'eurusd', 'DEXJPUS': 'usdjpy', 'DEXUSUK': 'gbpusd', 'DEXSZUS': 'usdchf',
        'DEXCAUS': 'usdcad', 'DEXUSAL': 'audusd', 'DEXUSNZ': 'nzdusd', 'DEXSDUS': 'usdsek',
        'DEXNOUS': 'usdnok',
        'DCOILWTICO': 'wti', 'DCOILBRENTEU': 'brent', 'DHHNGSP': 'natgas',
        'CBBTCUSD': 'bitcoin'}) }}
{% endmacro %}

{# The series that FRED gives in millions of dollars. core.rates holds them in billions. #}
{% macro fred_in_millions() %}
    {{ return(['WALCL', 'WSHOTSL', 'WSHOMCB', 'WRESBAL', 'WTREGEN']) }}
{% endmacro %}
