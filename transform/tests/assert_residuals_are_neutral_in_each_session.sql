-- The weighted regression makes the residuals of each session sum to 0 under the
-- regression weights, within each industry and so in total. A residual that does not
-- is a residual from the wrong factor returns (a date shift or a wrong join).
select date, sum(w * resid) / sum(w) as weighted_mean
from {{ ref('mart_residuals') }}
group by date
having abs(sum(w * resid) / sum(w)) > 1e-6
