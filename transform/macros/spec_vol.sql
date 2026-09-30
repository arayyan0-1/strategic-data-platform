{#
  The own volatility of a name, the first step of the forecast of its specific volatility.
  It is the root mean square of the residuals of the name before the row. The weight of a
  residual halves every spec_vol_half_life rows. It is null until the name has
  spec_vol_min_obs residuals before the row.

  The query needs a column resid, a column k (the position of the residual in the history
  of the name, from 1) and a window prev that holds the rows of the name before the row.
  One running sum holds each residual with the weight decay^(-k). The factor decay^(k - 1)
  turns the sum into the weights of row k. The weights of the k - 1 rows before add up to
  (1 - decay^(k - 1)) / (1 - decay).

  The weight decay^(-k) grows fast with k. A half-life below 2 leaves too little room
  before it overflows, about 2,000 rows.
#}
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
