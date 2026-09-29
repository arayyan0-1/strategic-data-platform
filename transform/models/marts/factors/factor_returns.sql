{#
  Every factor return in one long table: one row per session, family and factor. A
  row is dated on the session that earns the return, from the close before.

  - quintile: the top less the bottom quintile of each signal (signal_long_short).
  - style: the market and the eight pure style factors of the regression with industries
    (style_factor_returns).
  - industry: the Fama-French 12 industries of the same regression, each less the market.
  - pca: eigenportfolios of the correlation matrix (pca_factor_returns).
  - ff: the published Fama-French factors, with the short- and long-term reversal
    factors. They lag about two months.
  - ff_industry: the value-weighted Fama-French 12 industry portfolios, each less the
    market (mkt_rf + rf), so each compares with the industry family.
  - ff_mimic: the Fama-French factors mimicked in this universe (ff_mimic_returns).
  - q: the q5 factors of Hou, Xue and Zhang. A new vintage about once a year.
#}

with sessions as (

    select date, lead(date) over (order by date) as next_date
    from (select distinct date from {{ ref('signals') }})

), style as (

    unpivot (select date, market, size, liquidity, beta, momentum, reversal, volatility,
                    dividend_yield, high_52w
             from {{ ref('style_factor_returns') }})
    on market, size, liquidity, beta, momentum, reversal, volatility, dividend_yield, high_52w
    into name factor value ret

), industry as (

    -- Unknown collects the names with no SIC code yet. It is not an industry.
    select date, replace(factor, 'ind_', '') as factor, ret
    from (
        unpivot (select * exclude (n_names, r2, market, size, liquidity, beta, momentum,
                                   reversal, volatility, dividend_yield, high_52w, ind_unknown)
                 from {{ ref('style_factor_returns') }})
        on columns('^ind_')
        into name factor value ret
    )

), pca as (

    unpivot (select date, pc1, pc2, pc3, pc4, pc5 from {{ ref('pca_factor_returns') }})
    on pc1, pc2, pc3, pc4, pc5
    into name factor value ret

), ff as (

    unpivot (select date, mkt_rf, smb, hml, rmw, cma, mom, st_rev, lt_rev
             from {{ ref('stg_french__factors') }}
             where date >= (select min(date) from sessions))
    on mkt_rf, smb, hml, rmw, cma, mom, st_rev, lt_rev
    into name factor value ret

), ff_industry as (

    select date, replace(factor, 'ind_', '') as factor, ret - (mkt_rf + rf) as ret
    from (
        unpivot (select * from {{ ref('stg_french__factors') }}
                 where date >= (select min(date) from sessions))
        on columns('^ind_')
        into name factor value ret
    )

), q as (

    unpivot (select date, r_mkt as mkt, r_me as me, r_ia as ia, r_roe as roe, r_eg as eg
             from {{ ref('stg_global_q__factors') }}
             where date >= (select min(date) from sessions))
    on mkt, me, ia, roe, eg
    into name factor value ret

), ff_mimic as (

    unpivot (select date, mkt_rf, smb, hml, rmw, cma, mom
             from {{ ref('ff_mimic_returns') }})
    on mkt_rf, smb, hml, rmw, cma, mom
    into name factor value ret

), quintile as (

    -- The long-short mart is dated on the formation session. Its return is earned
    -- on the next session.
    select s.next_date as date, l.signal as factor, l.ret as ret
    from {{ ref('signal_long_short') }} l
    join sessions s on s.date = l.date
    where s.next_date is not null and l.ret is not null

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
select date, 'ff_industry', factor, ret from ff_industry
union all
select date, 'q', factor, ret from q
union all
select date, 'ff_mimic', factor, ret from ff_mimic
