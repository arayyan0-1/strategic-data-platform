{# The signal battery, named once. Add a factor here and in mart_signals. #}
{% macro signal_columns() -%}
reversal_1, reversal_5, momentum_12_1, vol_20, vol_60, amihud_20, beta_252, div_yield, overnight_ret, intraday_ret, hi_52w
{%- endmacro %}

{# The battery as a SQL list of names. #}
{% macro signal_list() -%}
[{% for s in signal_columns().split(',') %}'{{ s | trim }}'{{ ", " if not loop.last }}{% endfor %}]
{%- endmacro %}

{# The integer key of a signal name, and the name of a key. A window sorts on an
   integer key faster than on text. #}
{% macro signal_id(column) -%}
list_position({{ signal_list() }}, {{ column }})::utinyint
{%- endmacro %}

{% macro signal_name(column) -%}
{{ signal_list() }}[{{ column }}]
{%- endmacro %}
