{#
  One row per security and month-end: the market cap of the share class and the
  industry, from the monthly ticker details. The vendor market_cap is for the whole
  company (GOOG and GOOGL show the same value), so the cap here is the shares of the
  class times the close of the class. A pull with no class share count falls back to
  the vendor value, and cap_source says which rule fired.

  The class count can lag a split, give the count of another class, or be 1,000 times
  too small for some pulls (NFLX on 2025-11-28, HALO on 2025-08-29). A run of at most
  class_count_max_run class counts is refused when it jumps by more than
  class_count_max_ratio from the count before it, and the count after it agrees with the
  count before within class_count_agree_ratio. A run that agrees with the company count of
  each of its pulls stays (COF on 2025-05-30, after its merger). A refused run takes the
  last count that is not refused, on the split basis, and cap_source is class_carried. So
  the count after can refuse a class count, but it never sets the cap. A run with no count
  after it stays, because no field of one pull tells a lag from a real change.

  The vendor value can count other classes, or the old shares after a reorganization
  (WOLF on 2025-09-30). It is refused when every one of the nearest class counts before
  and after it in the same episode disagrees with it by more than a factor of
  company_cap_max_ratio, on one split basis. One count that agrees keeps the value,
  because the split table can miss a split (the HON reverse split of 2026-06-29). A
  refused value takes the class count of the pull before, if that count is not refused,
  and cap_source is class_carried. Otherwise cap is null and cap_source is refused. The
  count after is later data. It can refuse the vendor value only when no earlier count
  agrees, and it never sets the cap.

  industry is the Fama-French 12-industry class of the SIC code (seed ff12_industries).
  A code outside every range is Other, as in the library. A security with no code on
  any month-end so far is Unknown. The code carries forward from earlier month-ends,
  because the vendor leaves it out on some pulls.
#}

with snaps as (

    select
        d.ticker,
        d.date                                      as month_end,
        d.share_class_shares_outstanding            as shares,
        d.market_cap                                as company_cap,
        d.sic_code,
        d.sic_description,
        k.security_key,
        k.episode
    from {{ ref('stg_massive__ticker_details') }} d
    join {{ ref('int_tickers_keyed') }} k
        on k.ticker = d.ticker and k.date = d.date

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
    join {{ ref('int_prices_adjusted') }} p
        on p.ticker = s.ticker and p.date = s.month_end

), stepped as (

    select
        *,
        last_value(class_count ignore nulls) over (
            partition by security_key, ticker, episode order by month_end
            rows between unbounded preceding and 1 preceding
        )                                                                   as class_prev
    from priced

), jumps as (

    -- A new run starts at each class count that jumps from the one before it.
    select
        *,
        coalesce(abs(ln(class_count / class_prev))
                 > ln({{ var('class_count_max_ratio') }}), false)           as starts_run
    from stepped

), runs as (

    -- The count after a run is the first count of the next run. An open run has none.
    select
        *,
        sum(starts_run::int) over up_to_now                                 as run,
        first_value(case when starts_run then class_count end ignore nulls)
            over later                                                      as run_after
    from jumps
    window
        up_to_now as (partition by security_key, ticker, episode order by month_end
                      rows between unbounded preceding and current row),
        later     as (partition by security_key, ticker, episode order by month_end
                      rows between 1 following and unbounded following)

), bounded as (

    select
        *,
        count(class_count) over whole_run                                   as run_pulls,
        max(case when starts_run then class_prev end) over whole_run        as run_before,
        bool_and(abs(ln(class_count / company_count))
                 <= ln({{ var('class_count_agree_ratio') }})) over whole_run as run_agrees_company
    from runs
    window whole_run as (
        partition by security_key, ticker, episode, run order by month_end
        rows between unbounded preceding and unbounded following)

), classed as (

    select
        *,
        coalesce(
            class_count is not null
            and run_pulls <= {{ var('class_count_max_run') }}
            and abs(ln(run_before / run_after)) <= ln({{ var('class_count_agree_ratio') }})
            and not coalesce(run_agrees_company, false),
            false)                                                          as class_refused
    from bounded

), checked as (

    -- Only a class count that is not refused is evidence about the vendor value, and
    -- only such a count is carried.
    select
        *,
        last_value(case when not class_refused then class_count end ignore nulls)
            over earlier                                                    as count_before,
        first_value(case when not class_refused then class_count end ignore nulls)
            over later                                                      as count_after,
        lag(case when not class_refused then class_count end) over (
            partition by security_key, ticker, episode order by month_end
        )                                                                   as count_last_pull
    from classed
    window
        earlier as (partition by security_key, ticker, episode order by month_end
                    rows between unbounded preceding and 1 preceding),
        later   as (partition by security_key, ticker, episode order by month_end
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
        case when shares > 0 and class_refused and count_before > 0
                  then count_before * adj_close_split
             when shares > 0 then shares * close
             when company_cap > 0 and not company_disagrees then company_cap
             when company_cap > 0 and count_last_pull > 0
                  then count_last_pull * adj_close_split end                     as cap,
        case when shares > 0 and class_refused and count_before > 0 then 'class_carried'
             when shares > 0 then 'class_shares'
             when company_cap > 0 and company_disagrees and count_last_pull > 0
                  then 'class_carried'
             when company_cap > 0 and company_disagrees then 'refused'
             when company_cap > 0 then 'company_cap' end                         as cap_source
    from judged

), coded as (

    select
        *,
        last_value(sic_code ignore nulls) over (
            partition by security_key order by month_end
            rows between unbounded preceding and current row
        ) as sic_known
    from capped

)

select
    c.security_key,
    c.ticker,
    c.month_end,
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
    partition by c.security_key, c.month_end
    order by c.shares is null, length(c.ticker), c.ticker
) = 1
