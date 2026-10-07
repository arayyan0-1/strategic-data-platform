{# Own volatility of a name: the RMS of its earlier residuals, the weight halving every
   spec_vol_half_life rows; null until spec_vol_min_obs. Needs resid, k (position in the name's
   history, from 1) and a window prev over earlier rows. #}
{% macro own_vol() -%}
    {%- if var('spec_vol_half_life') < 2 -%}
        {{ exceptions.raise_compiler_error('spec_vol_half_life must be 2 or more.') }}
    {%- endif -%}
    {%- set decay = 0.5 ** (1.0 / var('spec_vol_half_life')) -%}
    case when k - 1 >= {{ var('spec_vol_min_obs') }}
         then sqrt(sum(resid * resid * power({{ decay }}, -k)) over prev
                   * power({{ decay }}, k - 1) * (1 - {{ decay }})
                   / (1 - power({{ decay }}, k - 1)))
    end
{%- endmacro %}
