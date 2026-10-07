{# Evaluation calendar. fold is 0 before the first universe session, 1..eval_folds for
   development (contiguous, about equal), eval_folds + 1 for the holdout. label_fold_h is the
   fold of the session h later. #}
{%- set horizons = [1, 5, 21] %}
{%- set k = var('eval_folds') | int %}

with sessions as (

    select
        date,
        bool_or(in_universe) as has_universe
    from {{ ref('int_universe') }}
    group by date

), periods as (

    select
        date,
        case
            when date >= cast('{{ var("holdout_start") }}' as date) then 'holdout'
            when date >= (select min(date) from sessions where has_universe)
                then 'development'
            else 'before'
        end as period
    from sessions

), folded as (

    select
        date,
        period,
        case period
            when 'development' then ntile({{ k }}) over (partition by period order by date)
            when 'holdout' then {{ k + 1 }}
            else 0
        end as fold
    from periods

)

select
    date,
    row_number() over (order by date) as session,
    period,
    fold,
    {%- for h in horizons %}
    lead(fold, {{ h }}) over (order by date) as label_fold_{{ h }}{{ "," if not loop.last }}
    {%- endfor %}
from folded
