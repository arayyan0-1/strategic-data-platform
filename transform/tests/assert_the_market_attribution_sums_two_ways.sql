{# A returned row is a session and weighting where the factor parts and the industry parts of the
   market return disagree. #}
select date, weighting,
       sum(contribution) filter (where kind = 'factor')   as by_factor,
       sum(contribution) filter (where kind = 'industry') as by_industry
from {{ ref('market_attribution') }}
group by date, weighting
having abs(by_factor - by_industry) > 1e-9 or by_factor is null or by_industry is null
