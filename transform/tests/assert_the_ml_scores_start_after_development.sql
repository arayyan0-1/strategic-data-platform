{# A returned row is a score for a month up to the end of development: the models are frozen on
   development, so they score only the months after it. #}
select fund, month, model
from {{ ref('fund_ml_scores') }}
where month <= date '{{ var("fund_signal_dev_end") }}'
