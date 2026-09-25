{#
  Adjusted prices from two as-of joins. The vendor factor is cumulative, so the
  adjustment is one lookup: for a price on D, take the first event strictly after
  D and multiply. Strictly after, not on, or every split gets one false return.
  Splits and dividends are independent factors and multiply.

  The vendor chains a factor inside one ticker only. When a security changes its
  ticker, the rows of the earlier ticker also take the factor that the later ticker
  starts from. Without that carry, the series of the security jumps at the change.
#}

with recursive bars as (

    select
        ticker,
        cast(date as date)  as date,
        open, high, low, close, volume, transactions
    from {{ lake('us_stocks_day_aggs', 'date') }}
    where ticker is not null

-- Bound events at the last bar. A later event has not happened inside the panel
-- and must not restate it, and a future ex-date has a null factor by construction.
), panel_end as (

    select max(date) as last_bar_date from bars

), splits as (

    select ticker, event_date, factor, is_collapsed
    from {{ ref('stg_corporate_actions') }}
    where kind = 'split'
      and event_date <= (select last_bar_date from panel_end)

), dividends as (

    select ticker, event_date, factor, is_collapsed
    from {{ ref('stg_corporate_actions') }}
    where kind = 'dividend'
      and event_date <= (select last_bar_date from panel_end)

), with_split as (

    select
        b.*,
        s.event_date    as next_split_date,
        s.factor        as next_split_factor,
        s.is_collapsed  as split_is_collapsed
    from bars b
    asof left join splits s
      on b.ticker = s.ticker
     and b.date < s.event_date

), with_both as (

    select
        w.*,
        d.event_date    as next_dividend_date,
        d.factor        as next_dividend_factor,
        d.is_collapsed  as dividend_is_collapsed
    from with_split w
    asof left join dividends d
      on w.ticker = d.ticker
     and w.date < d.event_date

), factors as (

    select
        *,
        -- No later event is a factor of 1.0. A later event with a null factor is
        -- unknown, not 1.0. Keep the two apart.
        case when next_split_date is null then 1.0 else next_split_factor end
            as own_split_factor,
        case when next_dividend_date is null then 1.0 else next_dividend_factor end
            as own_dividend_factor
    from with_both

), series as (

    -- The series of each security: its primary line on each session.
    select security_key, ticker, date
    from {{ ref('stg_security_lines') }}
    where is_primary_line

), segments as (

    -- A segment is a run of consecutive sessions of one ticker in the series.
    select security_key, ticker, min(date) as first_date, max(date) as last_date
    from (
        select
            *,
            row_number() over (partition by security_key order by date)
              - row_number() over (partition by security_key, ticker order by date) as run
        from series
    )
    group by security_key, ticker, run

), next_spans as (

    -- A ticker change: the next segment of the series has another ticker. A run of
    -- changes can come back to an earlier ticker (A to B to A).
    select *
    from (
        select
            security_key,
            ticker,
            last_date                                   as span_last_date,
            lead(ticker) over w                         as new_ticker,
            lead(first_date) over w                     as new_first_date
        from segments
        window w as (partition by security_key order by first_date)
    )
    where new_ticker is not null

), tails as (

    -- The old ticker can trade after its last session in the series, with a CIK key
    -- or no key when the vendor drops the FIGI. Those bars are still the old
    -- security. A bar under another FIGI ends the tail.
    select
        n.ticker,
        n.span_last_date,
        b.date,
        bool_and(
            k.security_key is null
            or k.security_key = n.security_key
            or k.key_rule in ('cik_ticker', 'ticker_only')
        ) over (
            partition by n.ticker, n.span_last_date order by b.date
            rows between unbounded preceding and current row
        ) as in_tail
    from next_spans n
    inner join bars b
        on b.ticker = n.ticker
       and b.date   > n.span_last_date
       and b.date   < n.new_first_date
    left join {{ ref('stg_tickers') }} k
        on k.ticker = b.ticker
       and k.date   = b.date

), changes as (

    select
        n.ticker,
        coalesce(max(t.date) filter (where t.in_tail), n.span_last_date) as old_last_date,
        n.new_ticker,
        n.new_first_date
    from next_spans n
    left join tails t
        on t.ticker         = n.ticker
       and t.span_last_date = n.span_last_date
    group by n.ticker, n.span_last_date, n.new_ticker, n.new_first_date

), events as (

    select ticker, event_date, kind, factor, is_collapsed
    from {{ ref('stg_corporate_actions') }}
    where event_date <= (select last_bar_date from panel_end)

), change_ends as (

    -- An event of the old ticker after its last bar, up to the first bar of the new
    -- ticker, is part of the change. The old rows already take it, so the carry starts
    -- after it, and a copy of the event on the new ticker does not count twice.
    select
        c.ticker,
        c.old_last_date,
        c.new_ticker,
        c.new_first_date,
        k.kind,
        coalesce(max(e.event_date), c.old_last_date) as change_end
    from changes c
    cross join (values ('split'), ('dividend')) k(kind)
    left join events e
        on e.ticker      = c.ticker
       and e.kind        = k.kind
       and e.event_date  > c.old_last_date
       and e.event_date <= c.new_first_date
    group by 1, 2, 3, 4, 5

), new_side as (

    -- The factor that the new ticker starts from.
    select
        c.*,
        n.event_date                        as step_event_date,
        n.factor                            as new_factor,
        coalesce(n.is_collapsed, false)     as step_collapsed
    from change_ends c
    asof left join events n
        on n.ticker = c.new_ticker
       and n.kind   = c.kind
       and c.change_end < n.event_date

), steps as (

    -- The old ticker can pass to another security after the change. The vendor chains
    -- those later events into the factor of the old ticker, so the step divides them out.
    select
        row_number() over (order by s.ticker, s.old_last_date, s.kind) as id,
        s.*,
        case when s.step_event_date is null then 1.0 else s.new_factor end
            / coalesce(o.factor, 1.0)       as step
    from new_side s
    asof left join (select * from events where factor is not null) o
        on o.ticker = s.ticker
       and o.kind   = s.kind
       and s.change_end < o.event_date

), linked as (

    -- The change that the new ticker makes next, if any.
    select s.*, nx.id as next_id
    from steps s
    asof left join steps nx
        on nx.ticker = s.new_ticker
       and nx.kind   = s.kind
       and s.new_first_date <= nx.old_last_date

), chain as (

    -- Multiply the steps along a run of changes (A to B to C).
    select id, next_id, step as carry, step_event_date as carry_event_date,
           step_collapsed as carry_collapsed
    from linked
    union all
    select c.id, l.next_id, c.carry * l.step,
           coalesce(c.carry_event_date, l.step_event_date),
           c.carry_collapsed or l.step_collapsed
    from chain c
    inner join linked l on l.id = c.next_id

), carries as (

    select l.ticker, l.old_last_date, l.new_ticker, l.kind,
           c.carry, c.carry_event_date, c.carry_collapsed
    from chain c
    inner join linked l on l.id = c.id
    where c.next_id is null

), with_split_carry as (

    -- A row takes the carry of the first change of its ticker on or after its date.
    select
        f.*,
        cs.new_ticker                       as later_ticker,
        cs.carry                            as split_carry,
        cs.carry_event_date                 as carry_split_date,
        coalesce(cs.carry_collapsed, false) as split_carry_collapsed
    from factors f
    asof left join (select * from carries where kind = 'split') cs
        on cs.ticker = f.ticker
       and f.date <= cs.old_last_date

), with_carries as (

    select
        w.*,
        cd.old_last_date is not null        as has_dividend_carry,
        cd.carry                            as dividend_carry,
        cd.carry_event_date                 as carry_dividend_date,
        coalesce(cd.carry_collapsed, false) as dividend_carry_collapsed
    from with_split_carry w
    asof left join (select * from carries where kind = 'dividend') cd
        on cd.ticker = w.ticker
       and w.date <= cd.old_last_date

), adjusted as (

    select
        *,
        own_split_factor * coalesce(split_carry, 1.0)       as split_factor,
        -- A null carry is an unknown dividend factor on the later ticker.
        own_dividend_factor
            * case when has_dividend_carry then dividend_carry else 1.0 end
                                                            as dividend_factor
    from with_carries

)

select
    ticker,
    date,

    -- Unadjusted price. A price floor screens on the traded price.
    open, high, low, close, volume, transactions,
    close * volume                              as dollar_volume,

    split_factor,
    dividend_factor,
    split_factor * dividend_factor              as total_factor,

    -- Split-adjusted: mechanical and complete.
    open  * split_factor                        as adj_open_split,
    high  * split_factor                        as adj_high_split,
    low   * split_factor                        as adj_low_split,
    close * split_factor                        as adj_close_split,
    volume / split_factor                       as adj_volume,

    -- Total-adjusted: the default column, null where the dividend factor is unknown.
    close * split_factor * dividend_factor      as adj_close_total,

    -- The next event that adjusts this row, on this ticker or on a later ticker.
    coalesce(next_split_date, carry_split_date)         as next_split_date,
    coalesce(next_dividend_date, carry_dividend_date)   as next_dividend_date,
    later_ticker,
    coalesce(split_is_collapsed, false)
        or coalesce(dividend_is_collapsed, false)
        or split_carry_collapsed
        or dividend_carry_collapsed             as factor_from_collapsed_event

from adjusted
