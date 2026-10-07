{# A returned row is a period where the Benjamini-Hochberg count is below the Bonferroni count or
   above the uncorrected count, or the model's n_tests is not the count of tests with a p-value. #}
with tests as (

    select period, count(p) as n_tests
    from {{ ref('market_seasonality') }}
    group by period

), counts as (

    select
        s.period,
        t.n_tests,
        min(s.n_tests)                                                  as n_tests_model,
        max(s.n_tests)                                                  as n_tests_model_max,
        count(*) filter (where s.significant)                           as n_significant,
        count(*) filter (where s.p < {{ var('seasonality_fdr') }} / t.n_tests) as n_bonferroni,
        count(*) filter (where s.p <= {{ var('seasonality_fdr') }})     as n_uncorrected
    from {{ ref('market_seasonality') }} s
    join tests t using (period)
    group by s.period, t.n_tests

)

select *
from counts
where n_significant < n_bonferroni
   or n_significant > n_uncorrected
   or n_tests_model <> n_tests
   or n_tests_model_max <> n_tests
