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

    -- One row per name and session, one rank column per signal.
    pivot (
        select date, security_key, signal, avg_rank
        from {{ ref('mart_signal_panel') }}
    )
    on signal in ({% for s in signals %}'{{ s }}'{{ ", " if not loop.last }}{% endfor %})
    using max(avg_rank)
    group by date, security_key

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
