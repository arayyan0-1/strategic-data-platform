{# The FRED series behind each macro factor return, and how the change over a session is
   measured: diff is the difference of the levels (percentage points for a yield or a
   spread), log is the difference of the natural logarithms. #}
{% macro macro_factors() %}
    {{ return({
        'd_t_10y': ['DGS10', 'diff'],
        'd_t_2y': ['DGS2', 'diff'],
        'd_real_10y': ['DFII10', 'diff'],
        'd_hy_oas': ['BAMLH0A0HYM2', 'diff'],
        'd_dollar': ['DTWEXBGS', 'log'],
        'd_wti': ['DCOILWTICO', 'log'],
        'd_bitcoin': ['CBBTCUSD', 'log'],
        'd_usdjpy': ['DEXJPUS', 'log'],
        'd_eurusd': ['DEXUSEU', 'log']}) }}
{% endmacro %}
