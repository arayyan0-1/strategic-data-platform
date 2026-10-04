{#
  The universe per date, from an instrument filter and a liquidity filter. Every
  threshold is a var. The liquidity window and the seasoning count belong to a
  security, not to a ticker. They run over its primary line, so a change of ticker
  continues them, and a new security on a used ticker starts them again. They count
  rows of the line. The liquidity window ends at D-1, because a window that includes D
  uses the volume of the day a position opens. A line that is not the primary line has
  no window and fails the filter.

  A session takes the details of the newest month-end on or before it. That month-end
  is the same for every security. So details_by_month_end holds the details of each
  security at each month-end, and a session joins on its own month-end. An as-of join
  would sort every price row.
#}

with keyed as (

    select * from {{ ref('int_tickers_keyed') }}

), bars as (

    select
        ticker,
        date,
        close,
        adj_close_split,
        dollar_volume
    from {{ ref('int_prices_adjusted') }}

), priced as (

    select
        l.ticker,
        l.date,
        l.security_key,
        l.is_primary_line,
        b.close,
        b.adj_close_split,
        b.dollar_volume
    from {{ ref('int_security_lines') }} l
    inner join bars b
        on b.ticker = l.ticker
       and b.date   = l.date

), liquidity as (

    -- A line that is not primary sits in its own partition, and its windows are
    -- dropped.
    select
        *,
        case when is_primary_line then avg(dollar_volume) over w end    as adv,
        case when is_primary_line then count(dollar_volume) over w end  as days_in_window,
        case when is_primary_line then count(*) over (
            partition by security_key, is_primary_line order by date
            rows between unbounded preceding and current row
        ) end                                                           as bars_seen
    from priced
    window w as (
        partition by security_key, is_primary_line order by date
        rows between {{ var('adv_window') }} preceding and 1 preceding
    )

), month_ends as (

    select distinct month_end from {{ ref('int_security_details') }}

), sessions as (

    -- The newest month-end on or before each session. A session before the first
    -- month-end takes a date at the start of time.
    select
        s.date,
        coalesce(m.month_end, date '0001-01-01')    as month_end
    from (select distinct date from {{ ref('int_tickers_keyed') }}) s
    asof left join month_ends m
        on m.month_end <= s.date

), details as (

    -- The next_ columns come from the row after. A session between two month-ends of a
    -- security reads them when the industry code of the row before is missing.
    select
        security_key,
        month_end,
        cap,
        adj_close_split,
        industry,
        industry_name,
        sic_code,
        lead(industry) over w         as next_industry,
        lead(industry_name) over w    as next_industry_name,
        lead(sic_code) over w         as next_sic_code
    from {{ ref('int_security_details') }}
    window w as (partition by security_key order by month_end)

    union all

    -- A session before the first month-end of a security reads the first row.
    select
        security_key,
        date '0001-01-01',
        null, null, null, null, null,
        industry,
        industry_name,
        sic_code
    from {{ ref('int_security_details') }}
    qualify row_number() over (partition by security_key order by month_end) = 1

), details_by_month_end as (

    select
        g.security_key,
        g.month_end,
        d.month_end                   as detail_month_end,
        d.cap,
        d.adj_close_split,
        d.industry,
        d.industry_name,
        d.sic_code,
        d.next_industry,
        d.next_industry_name,
        d.next_sic_code
    from (
        select s.security_key, m.month_end
        from (select distinct security_key from details) s
        cross join (select distinct month_end from sessions) m
    ) g
    asof left join details d
        on d.security_key = g.security_key
       and d.month_end   <= g.month_end

), joined as (

    select
        k.date,
        k.ticker,
        k.security_key,
        k.key_rule,
        p.is_primary_line,
        k.name,
        k.type,
        k.type_filled,
        k.primary_exchange,
        p.close,
        p.dollar_volume,
        p.adv,
        p.days_in_window,
        p.bars_seen,

        -- The market cap of the share class. It moves with the split-adjusted price
        -- from the month-end, and it is null when the month-end is over 70 days old.
        case when k.date - d.detail_month_end <= 70
             then d.cap * p.adj_close_split / d.adj_close_split end     as market_cap,
        -- The first month-end after gives the industry of a security that listed during
        -- the month. An industry code does not predict a return.
        coalesce(d.industry, d.next_industry, 'Unknown')                as industry,
        coalesce(d.industry_name, d.next_industry_name, 'Unknown')      as industry_name,
        case when k.date = d.detail_month_end
             then d.sic_code
             else coalesce(d.sic_code, d.next_sic_code) end             as sic_code,

        -- A null type, exchange or active flag fails the filter.
        coalesce(
            k.type_filled in ({{ sql_list('universe_types') }})
                and k.primary_exchange in ({{ sql_list('universe_exchanges') }})
                and k.active,
            false
        )                                                   as passes_instrument,

        -- A null input fails the check, so no flag is ever null.
        coalesce(p.close >= {{ var('min_price') }}, false)                  as passes_price,
        coalesce(p.adv >= {{ var('min_dollar_volume') }}, false)            as passes_adv,
        coalesce(p.days_in_window >= {{ var('min_days_in_window') }}, false) as passes_history,
        coalesce(p.bars_seen >= {{ var('min_days_since_first_bar') }}, false) as passes_seasoning

    from keyed k
    inner join liquidity p
        on p.ticker = k.ticker
       and p.date   = k.date
    left join sessions c
        on c.date = k.date
    left join details_by_month_end d
        on d.security_key = k.security_key
       and d.month_end   = c.month_end

), flagged as (

    select
        *,
        passes_price and passes_adv and passes_history and passes_seasoning as passes_liquidity
    from joined

)

select
    *,
    passes_instrument and passes_liquidity as in_universe
from flagged
