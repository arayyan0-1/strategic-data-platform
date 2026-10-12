{# Monthly total return in dollars of each index of the long history, and the US T-bill return of
   the month. index_key is region:<name>, country:<name>, us:<segment> or cash:tbill. A region
   earns its market less the risk-free rate plus the risk-free rate of its file; a country its
   market portfolio; a US segment a size-sorted portfolio of the library (the mean of three for
   us:small and us:big). A month with an input missing has no row. #}

with v as (

    select file, series, column_name, month, value
    from {{ ref('stg_french__international') }}

), tbill as (

    select month, value as rf
    from v
    where file = 'F-F_Research_Data_Factors_CSV.zip' and column_name = 'RF'

), region_files (index_key, file) as (

    values ('region:developed', 'Developed_5_Factors_CSV.zip'),
           ('region:developed_ex_us', 'Developed_ex_US_5_Factors_CSV.zip'),
           ('region:north_america', 'North_America_5_Factors_CSV.zip'),
           ('region:europe', 'Europe_5_Factors_CSV.zip'),
           ('region:japan', 'Japan_5_Factors_CSV.zip'),
           ('region:asia_pacific_ex_japan', 'Asia_Pacific_ex_Japan_5_Factors_CSV.zip'),
           ('region:emerging', 'Emerging_5_Factors_CSV.zip')

), region as (

    select
        r.index_key,
        v.month,
        max(v.value) filter (where v.column_name = 'Mkt-RF')
            + max(v.value) filter (where v.column_name = 'RF')       as ret
    from region_files r
    join v using (file)
    group by 1, 2

), country as (

    select
        'country:' || lower(replace(series, ' ', '_'))               as index_key,
        month,
        value                                                         as ret
    from v
    where file = 'F-F_International_Countries.zip' and column_name = 'Mkt'

), us_market as (

    select
        'us:market'                                                   as index_key,
        month,
        max(value) filter (where column_name = 'Mkt-RF')
            + max(value) filter (where column_name = 'RF')           as ret
    from v
    where file = 'F-F_Research_Data_Factors_CSV.zip'
    group by month

), picks (index_key, file, column_name) as (

    values ('us:small', '6_Portfolios_2x3_CSV.zip', 'SMALL LoBM'),
           ('us:small', '6_Portfolios_2x3_CSV.zip', 'ME1 BM2'),
           ('us:small', '6_Portfolios_2x3_CSV.zip', 'SMALL HiBM'),
           ('us:big', '6_Portfolios_2x3_CSV.zip', 'BIG LoBM'),
           ('us:big', '6_Portfolios_2x3_CSV.zip', 'ME2 BM2'),
           ('us:big', '6_Portfolios_2x3_CSV.zip', 'BIG HiBM'),
           ('us:value', '6_Portfolios_2x3_CSV.zip', 'BIG HiBM'),
           ('us:growth', '6_Portfolios_2x3_CSV.zip', 'BIG LoBM'),
           ('us:momentum', '6_Portfolios_ME_Prior_12_2_CSV.zip', 'BIG HiPRIOR'),
           ('us:quality', '6_Portfolios_ME_OP_2x3_CSV.zip', 'BIG HiOP'),
           ('us:low_vol', '25_Portfolios_ME_VAR_5x5_CSV.zip', 'BIG LoVAR')

), us_segment as (

    select
        p.index_key,
        v.month,
        avg(v.value)                                                  as ret
    from (select *, count(*) over (partition by index_key) as n_picks from picks) p
    join v using (file, column_name)
    group by p.index_key, v.month, p.n_picks
    having count(*) = p.n_picks

), cash as (

    select 'cash:tbill' as index_key, month, rf as ret
    from tbill

), returns as (

    select * from region
    union all select * from country
    union all select * from us_market
    union all select * from us_segment
    union all select * from cash

)

select
    r.index_key,
    r.month,
    r.ret,
    t.rf
from returns r
join tbill t using (month)
where r.ret is not null
order by r.index_key, r.month
