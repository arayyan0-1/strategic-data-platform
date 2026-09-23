{# The signal battery, named once. Add a factor here and in mart_signals. #}
{% macro signal_columns() -%}
reversal_1, reversal_5, momentum_12_1, vol_20, vol_60, amihud_20, beta_252, div_yield, overnight_ret, intraday_ret, hi_52w
{%- endmacro %}
