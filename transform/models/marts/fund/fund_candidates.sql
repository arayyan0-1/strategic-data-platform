{# The funds the paper fund may ever hold: the ETFs of the two screeners in core.etfs and the hand-checked
   funds of fund_extra_etfs (a seed fund the screeners also list keeps its screener row). Both get the
   same rules. role is cash for a T-bill fund, found by name, and equity for every other fund. A fund is
   out when its name marks an overlay (buffer, covered call, currency hedge), an active or rotation
   strategy, a screen, or a sector or theme. An equity fund must also be of an allowed asset class; a
   cash fund is fixed income by class, so the class rule skips it. excluded_by names the first rule
   that removed a fund, and is null for a candidate. The rules read the fund's stated mandate, never
   its returns. #}

with listed as (

    select
        fund,
        issuer,
        name,
        asset_class,
        sub_asset_class,
        region,
        country,
        expense_ratio,
        total_net_assets,
        'screener' as source
    from {{ ref('etfs') }}

    union all by name

    select
        fund,
        issuer,
        name,
        asset_class,
        sub_asset_class,
        region,
        country,
        expense_ratio,
        null::double as total_net_assets,
        'seed' as source
    from {{ ref('fund_extra_etfs') }} x
    where not exists (select 1 from {{ ref('etfs') }} e where e.fund = x.fund)

), funds as (

    select
        l.*,
        s.security_key,
        case when regexp_matches(l.name, '(?i)t-bill|treasury bill|0-3 month treasury')
             then 'cash' else 'equity' end as role
    from listed l
    left join (
        select ticker, security_key
        from {{ ref('securities') }}
        qualify row_number() over (partition by ticker order by last_date desc) = 1
    ) s on s.ticker = l.fund

), classified as (

    select
        *,
        case
            when role = 'equity' and asset_class not in ({{ sql_list('fund_asset_classes') }})
                                                                              then 'asset_class'
            when security_key is null                                         then 'no_prices'
            when regexp_matches(name, '(?i)buffer|buywrite|premium income|laddered|accelerated'
                                   || '|outcome|hedged')                      then 'overlay'
            when regexp_matches(name, '(?i)\bactive\b|rotation')              then 'active'
            when regexp_matches(name, '(?i)\besg\b|climate|carbon|paris|fossil|sustainable'
                                   || '|gender')                              then 'screened'
            when regexp_matches(name, '(?i)sector|\btech|semiconductor|software|biotech|health'
                                   || '|pharma|medical|financial|\bbank|broker|insurance'
                                   || '|capital markets|energy|\boil\b|\bgas\b|utilities'
                                   || '|industrials|materials|metals|mining|miners|\bgold\b'
                                   || '|silver|copper|lithium|consumer|retail|communication'
                                   || '|comm services|telecom|real estate|homebuilders'
                                   || '|home construction|transportation|aerospace|defense'
                                   || '|infrastructure|manufacturing|natural resources|timber'
                                   || '|agriculture|\bpower\b|preferred|a\.i\.|\bai\b'
                                   || '|innovation|future|exponential|clean|kensho|blockchain'
                                   || '|digital|cyber|genomics|neuroscience|self-driving'
                                   || '|environmental|breakthrough|fintech|independence'
                                   || '|texas|\btop (20|30)\b')                 then 'sector_or_theme'
        end as excluded_by
    from funds

)

select
    fund,
    security_key,
    issuer,
    name,
    role,
    source,
    asset_class,
    sub_asset_class,
    region,
    country,
    expense_ratio / 100.0       as expense_ratio,
    total_net_assets,
    excluded_by,
    excluded_by is null         as is_candidate
from classified
