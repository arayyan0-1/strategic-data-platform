{# A returned row is a label in signal_panel that reaches into the holdout for a signal whose
   holdout is closed. #}
{%- set holdout = var('eval_folds') | int + 1 %}

select p.signal, p.date, p.security_key
from {{ ref('signal_panel') }} p
join {{ ref('eval_sessions') }} e using (date)
where {{ holdout_closed('p.signal') }}
  and (
      (coalesce(e.label_fold_1, {{ holdout }}) = {{ holdout }}
       and (p.fwd_ret_1 is not null or p.fwd_resid_1 is not null))
   or (coalesce(e.label_fold_5, {{ holdout }}) = {{ holdout }}
       and (p.fwd_ret_5 is not null or p.fwd_resid_5 is not null))
   or (coalesce(e.label_fold_21, {{ holdout }}) = {{ holdout }}
       and (p.fwd_ret_21 is not null or p.fwd_resid_21 is not null))
  )
