{#
  Relations between assets that a trader reads as one number, from the monitor ETFs,
  one row per relation and session over two years. A ratio is the price of the first
  ETF over the second, rebased to 1 at the start. A correlation is over 63 sessions of
  daily returns. change_21 is the change over 21 sessions (a return for a ratio, a
  difference for a correlation). z_21 is change_21 over its own standard deviation in
  the window. pctile is the share of the window with a lower or equal value.

  Credit spreads, the VIX term structure and the dollar are measured, not proxied, in
  market_rates. stock_yield_corr is the correlation of SPY with the change of the
  10-year Treasury yield: negative when bad growth news drives yields, positive when
  inflation and rate news drive them (2022).
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
           ln(a.px / lag(a.px) over w) as ra, ln(b.px / lag(b.px) over w) as rb
    from defs d
    join px a on a.ticker = d.a
    join px b on b.ticker = d.b and b.date = a.date
    window w as (partition by d.relation order by a.date)

), valued as (

    select
        relation, label, kind, date,
        case kind
            when 'corr' then corr(ra, rb) over (partition by relation order by date
                                                rows between 62 preceding and current row)
            else (pa / pb) / first_value(pa / pb) over (partition by relation order by date)
        end as value
    from pairs

    union all

    select 'stock_yield_corr', 'Stocks vs Treasury yields, 63-day correlation (SPY, 10-year yield)',
           'corr', s.date,
           corr(s.r, y.dy_10y) over (order by s.date rows between 62 preceding and current row)
    from (select date, ln(px / lag(px) over (order by date)) as r
          from px where ticker = 'SPY') s
    join yields y using (date)

), finite as (

    -- A correlation over one row is NaN. A NULL keeps it out of the statistics.
    select relation, label, kind, date,
           case when isfinite(value) then value end as value
    from valued

), changed as (

    select
        *,
        case when kind = 'corr' then value - lag(value, 21) over w
             else value / lag(value, 21) over w - 1 end         as change_21
    from finite
    window w as (partition by relation order by date)

)

select
    *,
    change_21 / nullif(stddev_samp(change_21) over (partition by relation), 0) as z_21,
    percent_rank() over (partition by relation order by value)               as pctile
from changed
