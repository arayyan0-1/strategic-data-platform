{# A returned row is a pair of market_curve_pca components whose dot product is not 1 (same) or 0
   (different). #}
with p as (select component, measure, loading from {{ ref('market_curve_pca') }})
select a.component, b.component, sum(a.loading * b.loading) as dot
from p a join p b using (measure)
group by a.component, b.component
having abs(sum(a.loading * b.loading) - case when a.component = b.component then 1 else 0 end) > 1e-6
