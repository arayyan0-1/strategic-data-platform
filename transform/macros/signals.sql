{# The signal battery, named once. Add a factor here and in signals or residual_signals. #}
{% macro signal_columns() -%}
reversal_1, reversal_5, momentum_12_1, vol_20, vol_60, amihud_20, beta_252, dividend_yield, overnight_ret, intraday_ret, high_52w, short_ratio, days_to_cover, short_change, resid_reversal_1, resid_reversal_5, resid_reversal_21, resid_reversal_z_1, resid_reversal_z_5, resid_reversal_z_21
{%- endmacro %}

{# The columns of residual_signals, which signal_panel and signal_correlation join to signals. #}
{% macro residual_signal_columns() -%}
resid_reversal_1, resid_reversal_5, resid_reversal_21, resid_reversal_z_1, resid_reversal_z_5, resid_reversal_z_21
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

{# True when the holdout of the signal in `column` is closed: neither holdout_read nor
   read_holdout names it. #}
{% macro holdout_closed(column) -%}
{%- set open = var('holdout_read') + var('read_holdout') -%}
{%- if open -%}
not list_contains([{% for s in open %}'{{ s }}'{{ ", " if not loop.last }}{% endfor %}], {{ column }})
{%- else -%}
true
{%- endif -%}
{%- endmacro %}
