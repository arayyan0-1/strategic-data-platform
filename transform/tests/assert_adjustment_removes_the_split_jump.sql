-- The strongest statement about the boundary. Across a split, the unadjusted
-- series has a very large false return and the adjusted series does not.
-- An error of one day makes the adjusted count equal to or larger than the
-- unadjusted count. This test cannot fire on unusual data, because it compares
-- two counts on the same rows.
with steps as (
    select
        ticker,
        date,
        close,
        adj_close_split,
        lag(close)           over w as prev_close,
        lag(adj_close_split) over w as prev_adj,
        lag(date)            over w as prev_date
    from {{ ref('stg_prices_adjusted') }}
    window w as (partition by ticker order by date)
), boundaries as (
    select s.*
    from steps s
    join {{ ref('stg_corporate_actions') }} a
      on a.ticker = s.ticker
     and a.kind = 'split'
     and a.event_date > s.prev_date
     and a.event_date <= s.date
    where s.prev_close > 0 and s.prev_adj > 0
), counted as (
    select
        count(*) filter (abs(ln(close / prev_close))                 > ln(2)) as raw_jumps,
        count(*) filter (abs(ln(adj_close_split / prev_adj))         > ln(2)) as adj_jumps
    from boundaries
)
select * from counted where adj_jumps >= raw_jumps
