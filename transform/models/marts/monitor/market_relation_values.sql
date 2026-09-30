{#
  The relations between assets that a trader reads as one number, from the monitor ETFs,
  one row per relation and session over the whole history. A ratio is the price of the
  first ETF over the second. A correlation is over 63 sessions of daily returns, and it
  is null until the window is full. stock_yield_corr is the correlation of SPY with the
  change of the 10-year Treasury yield: negative when bad growth news drives yields,
  positive when inflation and rate news drive them (2022). change_1 is the change of one
  session: a log return for a ratio and a difference for a correlation.
#}

with px as (

    select ticker, date, adj_close as px from {{ ref('market_history') }}

), yields as (

    -- A row of core.rates holds the yield of the close before, so the change over
    -- session D is the row after D less the row of D.
    select date, lead(t_10y) over (order by date) - t_10y as dy_10y
    from {{ ref('rates') }}

), defs(relation, label, kind, a, b) as (

    values
        ('stock_bond_corr', 'Stocks vs bonds, 63-day correlation (SPY, TLT)', 'corr', 'SPY', 'TLT'),
        ('copper_gold', 'Growth: copper over gold (CPER / GLD)', 'ratio', 'CPER', 'GLD'),
        ('beta_lowvol', 'Risk appetite: high beta over low volatility (SPHB / SPLV)', 'ratio', 'SPHB', 'SPLV'),
        ('small_large', 'Size: small over large caps (IWM / SPY)', 'ratio', 'IWM', 'SPY'),
        ('value_growth', 'Style: value over growth (IWD / IWF)', 'ratio', 'IWD', 'IWF'),
        ('cyclical_defensive', 'Cyclicals over defensives (XLY / XLP)', 'ratio', 'XLY', 'XLP'),
        ('semis_market', 'Semiconductors over the market (SMH / SPY)', 'ratio', 'SMH', 'SPY'),
        ('banks_market', 'Regional banks over the market (KRE / SPY)', 'ratio', 'KRE', 'SPY'),
        ('equal_cap', 'Equal over cap weight (RSP / SPY)', 'ratio', 'RSP', 'SPY'),
        ('gold_stocks', 'Gold over stocks (GLD / SPY)', 'ratio', 'GLD', 'SPY')

), pairs as (

    select d.relation, d.label, d.kind, a.date, a.px as pa, b.px as pb,
           ln(a.px / lag(a.px) over w) as ra, ln(b.px / lag(b.px) over w) as rb,
           row_number() over w as n
    from defs d
    join px a on a.ticker = d.a
    join px b on b.ticker = d.b and b.date = a.date
    window w as (partition by d.relation order by a.date)

), valued as (

    select
        relation, label, kind, date,
        case kind
            when 'corr' then case when n > 63
                                  then corr(ra, rb) over (partition by relation order by date
                                                          rows between 62 preceding and current row) end
            else pa / pb
        end as value
    from pairs

    union all

    select 'stock_yield_corr', 'Stocks vs Treasury yields, 63-day correlation (SPY, 10-year yield)',
           'corr', s.date,
           case when s.n > 63
                then corr(s.r, y.dy_10y) over (order by s.date rows between 62 preceding and current row) end
    from (select date, ln(px / lag(px) over (order by date)) as r,
                 row_number() over (order by date) as n
          from px where ticker = 'SPY') s
    join yields y using (date)

), finite as (

    -- A correlation over one row is NaN. A NULL keeps it out of the statistics.
    select relation, label, kind, date,
           case when isfinite(value) then value end as value
    from valued

)

select
    *,
    case when kind = 'corr' then value - lag(value) over w
         else ln(value / lag(value) over w) end as change_1
from finite
window w as (partition by relation order by date)
