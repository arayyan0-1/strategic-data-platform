{#
  Every factor return in one long table: one row per session, family and factor. A
  row is dated on the session that earns the return, from the close before.

  - quintile: the top less the bottom quintile of each signal (mart_signal_ls_returns).
  - style: the market and the eight pure style factors of the regression with industries
    (mart_factor_style).
  - industry: the Fama-French 12 industries of the same regression, each less the market.
  - pca: eigenportfolios of the correlation matrix (mart_factor_pca).
  - ff: the published Fama-French factors. They lag about two months.
  - ff_mimic: the Fama-French factors mimicked in this universe (mart_factor_ff_mimic).
#}

with sessions as (

    select date, lead(date) over (order by date) as next_date
    from (select distinct date from {{ ref('mart_signals') }})

), style as (

    unpivot (select date, market, size, liquidity, beta, momentum, reversal, volatility,
                    dividend_yield, high_52w
             from {{ ref('mart_factor_style') }})
    on market, size, liquidity, beta, momentum, reversal, volatility, dividend_yield, high_52w
    into name factor value ret

), industry as (

    -- Unknown collects the names with no SIC code yet. It is not an industry.
    select date, replace(factor, 'ind_', '') as factor, ret
    from (
        unpivot (select * exclude (n_names, r2, market, size, liquidity, beta, momentum,
                                   reversal, volatility, dividend_yield, high_52w, ind_unknown)
                 from {{ ref('mart_factor_style') }})
        on columns('^ind_')
        into name factor value ret
    )

), pca as (

    unpivot (select date, pc1, pc2, pc3, pc4, pc5 from {{ ref('mart_factor_pca') }})
    on pc1, pc2, pc3, pc4, pc5
    into name factor value ret

), ff as (

    unpivot (select date, mkt_rf, smb, hml, rmw, cma, mom
             from {{ ref('stg_french_factors') }}
             where date >= (select min(date) from sessions))
    on mkt_rf, smb, hml, rmw, cma, mom
    into name factor value ret

), ff_mimic as (

    unpivot (select date, mkt_rf, smb, hml, rmw, cma, mom
             from {{ ref('mart_factor_ff_mimic') }})
    on mkt_rf, smb, hml, rmw, cma, mom
    into name factor value ret

), quintile as (

    -- The long-short mart is dated on the formation session. Its return is earned
    -- on the next session.
    select s.next_date as date, l.signal as factor, l.ls_ret as ret
    from {{ ref('mart_signal_ls_returns') }} l
    join sessions s on s.date = l.date
    where s.next_date is not null and l.ls_ret is not null

)

select date, 'quintile' as family, factor, ret from quintile
union all
select date, 'style', factor, ret from style
union all
select date, 'industry', factor, ret from industry
union all
select date, 'pca', factor, ret from pca
union all
select date, 'ff', factor, ret from ff
union all
select date, 'ff_mimic', factor, ret from ff_mimic
