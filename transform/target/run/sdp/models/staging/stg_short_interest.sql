
    

    create  table
      "sdp"."main_staging"."stg_short_interest__dbt_tmp"
  
    
    as (
      

with sessions as (

    select
        date as session,
        row_number() over (order by date) as rn
    from (select distinct date from read_parquet(
        '/Users/aray/strategic-data-platform/data/raw/us_stocks_day_aggs/date=*/data.parquet',
        hive_partitioning = true
    ))

),

raw as (

    select
        cast(settlement_date as date) as settlement_date,
        ticker,
        short_interest,
        avg_daily_volume,
        days_to_cover
    from read_parquet(
        '/Users/aray/strategic-data-platform/data/raw/massive_short_interest/date=*/data.parquet',
        hive_partitioning = true
    )

),

-- The first session strictly after each settlement date. Restricted to the
-- settlement dates that the bar calendar actually covers, so that the count
-- never starts from the first bar instead of from the settlement.
first_session_after as (

    select
        r.settlement_date,
        min(s.rn) as rn_first
    from (select distinct settlement_date from raw) r
    join sessions s
      on s.session > r.settlement_date
    where r.settlement_date >= (select min(session) from sessions)
    group by 1

),

publication as (

    select
        f.settlement_date,
        s.session as effective_date
    from first_session_after f
    left join sessions s
      on s.rn = f.rn_first + 8 - 1

)

select
    r.settlement_date,
    p.effective_date,
    r.ticker,
    r.short_interest,
    r.avg_daily_volume,

    -- Null, because the cap is not a measurement. See the header.
    case when r.days_to_cover < 999.99 then r.days_to_cover end as days_to_cover,
    r.days_to_cover >= 999.99                                   as days_to_cover_is_censored,

    -- The ratio computed from the two raw columns. Uncensored, and null only
    -- where the vendor reports no volume at all.
    r.short_interest / nullif(r.avg_daily_volume, 0)            as days_to_cover_computed

from raw r
left join publication p
    on p.settlement_date = r.settlement_date
    );
    
  