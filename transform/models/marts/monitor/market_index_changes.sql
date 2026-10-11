{# Constituent adds and drops of each listed equity ETF between consecutive as-of dates, over
   the last 45 days. A line is its security_key, else its ticker; cash and derivatives and
   zero weights are left out. #}

with funds as (

    select fund from {{ ref('etf_holdings_funds') }}
    where asset_class in ('Equity', 'Real Estate')

), lines as (

    select fund, date, coalesce(security_key, 'T:' || ticker) as line, ticker, name, weight
    from {{ ref('int_etf_holdings') }}
    where fund in (select fund from funds)
      and ticker is not null
      and coalesce(asset_class, 'Equity') = 'Equity'
      and weight > 0
      and date >= (select max(date) - 45 from {{ ref('int_etf_holdings') }})

), dates as (

    select fund, date, lag(date) over (partition by fund order by date) as prev_date
    from (select distinct fund, date from lines)

), adds as (

    select d.fund, d.date, 'add' as change, n.ticker, n.name, n.weight
    from dates d
    join lines n on n.fund = d.fund and n.date = d.date
    left join lines o on o.fund = d.fund and o.date = d.prev_date and o.line = n.line
    where d.prev_date is not null and o.line is null

), drops as (

    select d.fund, d.date, 'drop' as change, o.ticker, o.name, o.weight
    from dates d
    join lines o on o.fund = d.fund and o.date = d.prev_date
    left join lines n on n.fund = d.fund and n.date = d.date and n.line = o.line
    where n.line is null

)

select * from adds
union all
select * from drops
