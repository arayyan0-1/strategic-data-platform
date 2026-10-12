{# The funds the paper fund may ever hold: broad ETFs of the two screeners in fund_asset_classes.
   A fund is out when it holds one country other than the US, or when its name marks an
   overlay (buffer, covered call, currency hedge), an active or rotation strategy, a screen, or
   a sector or theme. excluded_by names the first rule that removed it, and is null for a
   candidate. The rules read the fund's stated mandate, never its returns. #}

with funds as (

    select
        e.*,
        s.security_key
    from {{ ref('etfs') }} e
    left join (
        select ticker, security_key
        from {{ ref('securities') }}
        qualify row_number() over (partition by ticker order by last_date desc) = 1
    ) s on s.ticker = e.fund

), classified as (

    select
        *,
        case
            when asset_class not in ({{ sql_list('fund_asset_classes') }}) then 'asset_class'
            when security_key is null then 'no_prices'
            when coalesce(country, 'Broad') not in ('Broad', 'United States')
              or regexp_matches(name, '(?i)\bperu\b|\bireland\b|\bbic\b')
              or (regexp_matches(name, '(?i)\bchina\b')
                  and not regexp_matches(name, '(?i)\bex[- ]china\b'))     then 'single_country'
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
    asset_class,
    sub_asset_class,
    region,
    country,
    expense_ratio / 100.0       as expense_ratio,
    total_net_assets,
    excluded_by,
    excluded_by is null         as is_candidate
from classified
