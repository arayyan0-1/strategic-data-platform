{#
  One row per security and month-end: the market cap of the share class and the
  industry, from the monthly ticker details. The vendor market_cap is for the whole
  company (GOOG and GOOGL show the same value), so the cap here is the shares of the
  class times the close of the class. A pull with no class share count falls back to
  the vendor value, and cap_source says which rule fired.

  industry is the Fama-French 12-industry class of the SIC code (seed ff12_industries).
  A code outside every range is Other, as in the library. A security with no code on
  any month-end so far is Unknown. The code carries forward from earlier month-ends,
  because the vendor leaves it out on some pulls.
#}

with snaps as (

    select
        d.ticker,
        cast(d.date as date)                        as snap_date,
        d.share_class_shares_outstanding            as shares,
        d.market_cap                                as company_cap,
        nullif(d.sic_code, '')                      as sic_code,
        d.sic_description,
        k.security_key
    from {{ lake('massive_ticker_details', 'date') }} d
    join {{ ref('stg_tickers') }} k
        on k.ticker = d.ticker and k.date = cast(d.date as date)

), priced as (

    select
        s.*,
        p.close,
        p.adj_close_split,
        coalesce(s.shares * p.close, s.company_cap)  as cap,
        case when s.shares > 0 then 'class_shares'
             when s.company_cap > 0 then 'company_cap' end as cap_source
    from snaps s
    join {{ ref('stg_prices_adjusted') }} p
        on p.ticker = s.ticker and p.date = s.snap_date

), coded as (

    select
        *,
        last_value(sic_code ignore nulls) over (
            partition by security_key order by snap_date
            rows between unbounded preceding and current row
        ) as sic_known
    from priced

)

select
    c.security_key,
    c.ticker,
    c.snap_date,
    c.shares,
    c.cap,
    c.cap_source,
    c.adj_close_split,
    c.sic_known                                          as sic_code,
    c.sic_description,
    coalesce(i.industry, case when c.sic_known is not null then 'Other' end, 'Unknown')
                                                         as industry,
    coalesce(i.industry_name, case when c.sic_known is not null then 'Other' end, 'Unknown')
                                                         as industry_name
from coded c
left join {{ ref('ff12_industries') }} i
    on try_cast(c.sic_known as integer) between i.sic_from and i.sic_to
-- A when-issued line can have details beside the regular line. Keep one row.
qualify row_number() over (
    partition by c.security_key, c.snap_date
    order by c.shares is null, length(c.ticker), c.ticker
) = 1
