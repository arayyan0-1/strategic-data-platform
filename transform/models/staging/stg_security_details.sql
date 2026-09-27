{#
  One row per security and month-end: the market cap of the share class and the
  industry, from the monthly ticker details. The vendor market_cap is for the whole
  company (GOOG and GOOGL show the same value), so the cap here is the shares of the
  class times the close of the class. A pull with no class share count falls back to
  the vendor value, and cap_source says which rule fired.

  The vendor value can count other classes, or the old shares after a reorganization
  (WOLF on 2025-09-30). It is refused when every one of the nearest class counts before
  and after it in the same episode disagrees with it by more than a factor of
  company_cap_max_ratio, on one split basis. Then cap is null and cap_source is refused.
  One count that agrees keeps the value, because one class count can itself be wrong
  (HON on 2026-06-30). The count after is later data. It can refuse the vendor value
  only when no earlier count agrees, and it never sets the cap.

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
        k.security_key,
        k.episode
    from {{ lake('massive_ticker_details', 'date') }} d
    join {{ ref('stg_tickers') }} k
        on k.ticker = d.ticker and k.date = cast(d.date as date)

), priced as (

    -- The share counts on the split basis of the last bar, so that two counts on the
    -- two sides of a split compare.
    select
        s.*,
        p.close,
        p.adj_close_split,
        case when s.shares > 0 and p.adj_close_split > 0
             then s.shares * p.close / p.adj_close_split end       as class_count,
        case when s.company_cap > 0 and p.adj_close_split > 0
             then s.company_cap / p.adj_close_split end            as company_count
    from snaps s
    join {{ ref('stg_prices_adjusted') }} p
        on p.ticker = s.ticker and p.date = s.snap_date

), checked as (

    select
        *,
        last_value(class_count ignore nulls) over earlier            as count_before,
        first_value(class_count ignore nulls) over later             as count_after
    from priced
    window
        earlier as (partition by security_key, ticker, episode order by snap_date
                    rows between unbounded preceding and 1 preceding),
        later   as (partition by security_key, ticker, episode order by snap_date
                    rows between 1 following and unbounded following)

), judged as (

    -- A missing count gives no evidence. The value is refused only when some count
    -- exists and each count that exists disagrees.
    select
        *,
        coalesce(
            (count_before is not null or count_after is not null)
            and (count_before is null
                 or abs(ln(company_count / count_before)) > ln({{ var('company_cap_max_ratio') }}))
            and (count_after is null
                 or abs(ln(company_count / count_after)) > ln({{ var('company_cap_max_ratio') }})),
            false)                                                   as company_disagrees
    from checked

), capped as (

    select
        *,
        case when shares > 0 then shares * close
             when company_cap > 0 and not company_disagrees then company_cap end as cap,
        case when shares > 0 then 'class_shares'
             when company_cap > 0 and company_disagrees then 'refused'
             when company_cap > 0 then 'company_cap' end                         as cap_source
    from judged

), coded as (

    select
        *,
        last_value(sic_code ignore nulls) over (
            partition by security_key order by snap_date
            rows between unbounded preceding and current row
        ) as sic_known
    from capped

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
