{# Monthly total return in dollars of each index of the long history, and the US T-bill return of
   the month. index_key is region:<name>, country:<name>, us:<segment>, devx:<segment>,
   em:<segment>, cash:tbill, or an asset class key. A region earns its market less the risk-free
   rate plus the risk-free rate of its file; a country its market portfolio; a segment a
   size-sorted portfolio of the library (the mean of three for a small segment and us:big), in
   the US, the developed markets outside it (devx) or the emerging markets (em). A month with an
   input missing has no row.

   The asset classes use the value at the end of the month: the last observation of a daily FRED
   series, the value of the month for a monthly one (the corporate yields and the World Bank
   prices are averages of the month, which smooths their returns).
   - bond:ust_2y, _5y, _10y, _30y, bond:corp_aaa, bond:corp_baa: a par bond with semiannual
     coupons, bought at the yield of the month before and priced at the yield of the month with
     one month less to maturity (macro par_bond_return). The Treasuries are the constant
     maturity yields of 2, 5, 10 and 30 years; the corporate bonds have 20 years.
   - bond:tips_10y: the same from the 10-year TIPS real yield, plus the change of the CPI in the
     month (the inflation accrual, added, not compounded).
   - gold, silver, platinum: the change of the World Bank price.
   - commodity:all, energy, metals, agriculture, base_metals, and the single commodities wti,
     natgas, copper, corn, wheat (US soft red winter), soybeans and sugar: the change of the World
     Bank price or index plus the T-bill return of the month, as collateral. A spot price has no
     roll yield.
   - crypto:bitcoin: the change of the Coinbase bitcoin price. #}

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
           ('us:low_vol', '25_Portfolios_ME_VAR_5x5_CSV.zip', 'BIG LoVAR'),
           ('devx:value', 'Developed_ex_US_6_Portfolios_ME_BE-ME_CSV.zip', 'BIG HiBM'),
           ('devx:small', 'Developed_ex_US_6_Portfolios_ME_BE-ME_CSV.zip', 'SMALL LoBM'),
           ('devx:small', 'Developed_ex_US_6_Portfolios_ME_BE-ME_CSV.zip', 'ME1 BM2'),
           ('devx:small', 'Developed_ex_US_6_Portfolios_ME_BE-ME_CSV.zip', 'SMALL HiBM'),
           ('devx:momentum', 'Developed_ex_US_6_Portfolios_ME_Prior_12_2_CSV.zip', 'BIG HiPRIOR'),
           ('em:value', 'Emerging_Markets_6_Portfolios_ME_BE-ME_CSV.zip', 'BIG HiBM'),
           ('em:small', 'Emerging_Markets_6_Portfolios_ME_BE-ME_CSV.zip', 'SMALL LoBM'),
           ('em:small', 'Emerging_Markets_6_Portfolios_ME_BE-ME_CSV.zip', 'ME1 BM2'),
           ('em:small', 'Emerging_Markets_6_Portfolios_ME_BE-ME_CSV.zip', 'SMALL HiBM'),
           ('em:momentum', 'Emerging_Markets_6_Portfolios_ME_Prior_12_2_CSV.zip', 'BIG HiPRIOR')

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

), fred_month as (

    select series_id, last_day(date) as month, arg_max(value, date) as value
    from {{ ref('stg_fred__series') }}
    where value is not null
      and series_id in ('DGS2', 'DGS5', 'DGS10', 'DGS30', 'DFII10', 'AAA', 'BAA', 'CPIAUCSL',
                        'CBBTCUSD')
    group by 1, 2

), bond_specs (index_key, series_id, years) as (

    values ('bond:ust_2y', 'DGS2', 2),
           ('bond:ust_5y', 'DGS5', 5),
           ('bond:ust_10y', 'DGS10', 10),
           ('bond:ust_30y', 'DGS30', 30),
           ('bond:tips_10y', 'DFII10', 10),
           ('bond:corp_aaa', 'AAA', 20),
           ('bond:corp_baa', 'BAA', 20)

), bond as (

    select
        b.index_key,
        y.month,
        {{ par_bond_return('p.value / 100', 'y.value / 100', 'b.years') }}   as ret
    from bond_specs b
    join fred_month y on y.series_id = b.series_id
    join fred_month p
        on p.series_id = b.series_id and p.month = last_day(y.month - interval 1 month)

), inflation as (

    select c.month, c.value / p.value - 1                          as infl
    from fred_month c
    join fred_month p
        on p.series_id = c.series_id and p.month = last_day(c.month - interval 1 month)
    where c.series_id = 'CPIAUCSL'

), tips as (

    select b.index_key, b.month, b.ret + i.infl                    as ret
    from bond b
    join inflation i using (month)
    where b.index_key = 'bond:tips_10y'

), bitcoin as (

    select
        'crypto:bitcoin'                                           as index_key,
        c.month,
        c.value / p.value - 1                                      as ret
    from fred_month c
    join fred_month p
        on p.series_id = c.series_id and p.month = last_day(c.month - interval 1 month)
    where c.series_id = 'CBBTCUSD'

), wb_change as (

    select
        c.series,
        c.month,
        c.value / p.value - 1                                      as chg
    from {{ ref('stg_worldbank__commodities') }} c
    join {{ ref('stg_worldbank__commodities') }} p
        on p.series = c.series and p.month = last_day(c.month - interval 1 month)

), metal_series (index_key, series) as (

    values ('gold', 'Gold'), ('silver', 'Silver'), ('platinum', 'Platinum')

), gold as (

    select k.index_key, w.month, w.chg as ret
    from metal_series k
    join wb_change w using (series)

), commodity_series (index_key, series) as (

    values ('commodity:all', 'Total Index'),
           ('commodity:energy', 'Energy'),
           ('commodity:metals', 'Metals & Minerals'),
           ('commodity:agriculture', 'Agriculture'),
           ('commodity:base_metals', 'Base Metals (ex. iron ore)'),
           ('commodity:wti', 'Crude oil, WTI'),
           ('commodity:natgas', 'Natural gas, US'),
           ('commodity:copper', 'Copper'),
           ('commodity:corn', 'Maize'),
           ('commodity:wheat', 'Wheat, US SRW'),
           ('commodity:soybeans', 'Soybeans'),
           ('commodity:sugar', 'Sugar, world')

), commodity as (

    select k.index_key, w.month, w.chg + t.rf                      as ret
    from commodity_series k
    join wb_change w using (series)
    join tbill t using (month)

), returns as (

    select * from region
    union all select * from country
    union all select * from us_market
    union all select * from us_segment
    union all select * from cash
    union all select * from bond where index_key <> 'bond:tips_10y'
    union all select * from tips
    union all select * from gold
    union all select * from commodity
    union all select * from bitcoin

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
