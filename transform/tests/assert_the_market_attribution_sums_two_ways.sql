-- The factor parts and the industry parts of the market return are two splits of one
-- sum, so they agree on each session and weighting. A missing part or a name counted
-- twice breaks the agreement.
select date, weighting,
       sum(contribution) filter (where kind = 'factor')   as by_factor,
       sum(contribution) filter (where kind = 'industry') as by_industry
from {{ ref('market_attribution') }}
group by date, weighting
having abs(by_factor - by_industry) > 1e-9 or by_factor is null or by_industry is null
