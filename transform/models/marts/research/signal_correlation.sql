{#
  The average cross-sectional rank correlation between each pair of signals. A
  pair near one is one bet under two names. Upper triangle and diagonal only,
  with the pair in alphabetical order. The rank is the panel avg_rank. Tied
  values get the average of the ranks that they occupy. The correlation of a
  session uses the names that have both signals.
#}
{%- set signals = signal_columns().split(',') | map('trim') | sort -%}
{%- set pairs = [] -%}
{%- for a in signals -%}{%- for b in signals if a <= b -%}
    {%- do pairs.append((a, b)) -%}
{%- endfor -%}{%- endfor %}

with wide as (

    -- One row per name and session, one rank column per signal. The rank is the
    -- panel rank, found again with the same rule from the in_universe rows of
    -- signals. A rank window for each signal costs less than a pivot of the panel.
    select
        date,
        {%- for s in signals %}
        case when {{ s }} is not null
             then (rank() over w_{{ s }} + count(*) over w_{{ s }}) / 2.0 end as {{ s }}
        {{- "," if not loop.last }}
        {%- endfor %}
    from (
        select date, {{ signals | join(', ') }}
        from {{ ref('signals') }}
        where in_universe
    )
    window
        {%- for s in signals %}
        w_{{ s }} as (partition by date order by {{ s }} nulls last
                      range between unbounded preceding and current row)
        {{- "," if not loop.last }}
        {%- endfor %}

), per_day as (

    select
        date,
        {%- for a, b in pairs %}
        corr({{ a }}, {{ b }})     as c_{{ loop.index }},
        count({{ a }} + {{ b }})   as n_{{ loop.index }}{{ "," if not loop.last }}
        {%- endfor %}
    from wide
    group by date

), long as (

    select unnest([
        {%- for a, b in pairs %}
        {signal_a: '{{ a }}', signal_b: '{{ b }}', c: c_{{ loop.index }}, n: n_{{ loop.index }}}
        {{- "," if not loop.last }}
        {%- endfor %}
    ], recursive := true)
    from per_day

)

-- A session in which no name has both signals has no correlation.
select
    signal_a,
    signal_b,
    avg(c)   as rank_corr,
    count(*) as n_days
from long
where n > 0
group by signal_a, signal_b
order by signal_a, signal_b
