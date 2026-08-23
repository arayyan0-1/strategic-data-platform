
    

    create  table
      "sdp"."main_staging"."stg_universe__dbt_tmp"
  
    
    as (
      

with reference as (

    select
        ticker,
        cast(date as date)      as date,
        name,
        type,
        primary_exchange,
        active,
        currency_name,
        cik,
        composite_figi,
        share_class_figi
    from read_parquet(
        '/Users/aray/strategic-data-platform/data/raw/massive_tickers/date=*/data.parquet',
        hive_partitioning = true
    )

), keyed as (

    select
        *,
        
        coalesce(
            share_class_figi,
            composite_figi,
            nullif(cik, '') || '.' || ticker,
            'TICKER.' || ticker
        ) as security_key,
        case
            when share_class_figi is not null then 'share_class_figi'
            when composite_figi   is not null then 'composite_figi'
            when nullif(cik, '')  is not null then 'cik_ticker'
            else 'ticker_only'
        end as key_rule
    from reference

), bars as (

    select
        ticker,
        date,
        close,
        dollar_volume
    from "sdp"."main_staging"."stg_prices_adjusted"

), liquidity as (

    select
        ticker,
        date,
        close,
        dollar_volume,
        avg(dollar_volume) over w      as adv,
        count(dollar_volume) over w    as days_in_window,
        count(*) over (
            partition by ticker order by date
            rows between unbounded preceding and current row
        )                              as bars_seen
    from bars
    window w as (
        partition by ticker order by date
        rows between 20 preceding and 1 preceding
    )

), joined as (

    select
        k.date,
        k.ticker,
        k.security_key,
        k.key_rule,
        k.name,
        k.type,
        k.primary_exchange,
        t.close,
        t.dollar_volume,
        t.adv,
        t.days_in_window,
        t.bars_seen,

        k.type in ('CS')
            and k.primary_exchange in ('XNYS','XNAS','XASE')
            and k.active                                    as passes_instrument,

        t.close        >= 5.0             as passes_price,
        t.adv          >= 1000000     as passes_adv,
        t.days_in_window >= 15  as passes_history,
        t.bars_seen    >= 60 as passes_seasoning

    from keyed k
    inner join liquidity t
        on k.ticker = t.ticker
       and k.date   = t.date

)

select
    *,
    coalesce(passes_price, false)
        and coalesce(passes_adv, false)
        and coalesce(passes_history, false)
        and coalesce(passes_seasoning, false)   as passes_liquidity,
    passes_instrument
        and coalesce(passes_price, false)
        and coalesce(passes_adv, false)
        and coalesce(passes_history, false)
        and coalesce(passes_seasoning, false)   as in_universe
from joined
    );
    
  