{# A returned row is a session where the residuals do not sum to 0 under the regression weights
   (a date shift or wrong join). #}
select date, sum(weight * resid) / sum(weight) as weighted_mean
from {{ ref('style_residuals') }}
group by date
having abs(sum(weight * resid) / sum(weight)) > 1e-6
