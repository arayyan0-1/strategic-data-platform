{# The funds the paper fund may ever hold: the ETFs of the two screeners in core.etfs and the hand-checked
   funds of fund_extra_etfs (a seed fund the screeners also list keeps its screener row). Both get the
   same rules. role is found from the name and the asset class:
   - cash: a T-bill fund, by name, in any class;
   - bond: asset class Fixed Income;
   - gold: asset class Commodity with gold in the name;
   - commodity: any other fund of class Commodity;
   - bitcoin: asset class Digital Assets with bitcoin in the name;
   - equity: every other fund.
   Equity and cash funds follow the rules they always had: an equity fund must be of an allowed asset
   class (a cash fund is fixed income by class, so the class rule skips it), and the name rules remove
   an overlay (buffer, covered call, currency hedge), an active or rotation strategy, a screen, or a
   sector or theme. The other roles follow a rule of their own, written as a list of what is in (a
   fund that fits no listed kind stays out) and a list of what is out:
   - bond is in for a government, TIPS, corporate, high yield, aggregate or core, mortgage-backed,
     international or emerging-market fund. It is out when it is municipal, a defined-maturity or
     ladder fund, an overlay (buywrite, premium income, interest-rate or inflation hedged),
     active, systematic or tactical, screened, convertible, preferred, floating-rate, a loan or
     CLO fund, leveraged or inverse, or has a maturity under one year (the cash funds are the
     T-bill funds);
   - gold is out for a miner, a strategy or futures fund, an overlay or a leveraged fund;
   - commodity is in for a broad commodity index fund, and out for a single commodity, an equity fund
     of producers, an overlay or a leveraged fund;
   - bitcoin is in for a spot fund, and out for a futures, income, ether or leveraged fund.
   excluded_by names the first rule that removed a fund, and is null for a candidate. The role of an
   excluded fund is the family its asset class belongs to. The rules read the fund's stated mandate,
   never its returns. #}

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
        case
            when regexp_matches(l.name, '(?i)t-bill|treasury bill|0-3 month treasury')
                                                                       then 'cash'
            when l.asset_class = 'Fixed Income'                        then 'bond'
            when l.asset_class = 'Commodity' and regexp_matches(l.name, '(?i)\bgold\b')
                                                                       then 'gold'
            when l.asset_class = 'Commodity'                           then 'commodity'
            when l.asset_class = 'Digital Assets' and regexp_matches(l.name, '(?i)bitcoin')
                                                                       then 'bitcoin'
            else 'equity'
        end as role
    from listed l
    left join (
        select ticker, security_key
        from {{ ref('securities') }}
        qualify row_number() over (partition by ticker order by last_date desc) = 1
    ) s on s.ticker = l.fund

), kinds as (

    select
        *,
        (   regexp_matches(name, '(?i)treasury|government|agency|\btips\b|inflation|aggregate|\bcore\b'
                              || '|universal|fixed income market|corporate|credit|investment grade'
                              || '|high yield|\bmbs\b|mortgage|gnma|cmbs|international|emerging')
            or regexp_matches(coalesce(sub_asset_class, ''),
                              '(?i)^(government|credit|corporates?|corporate and credit|high yield'
                              || '|inflation|inflation linked|mortgages|cmbs|aggregate)$'))
        and not regexp_matches(name, '(?i)muni|tax-exempt|ibonds|\bmy20\d\d|\b20[2-9]\d\b|ladder'
                                  || '|bulletshares|buywrite|premium income|buffer|interest rate hedged'
                                  || '|inflation hedged|\bactive\b|tactical|rotation|opportunistic'
                                  || '|systematic|yield optimized|public & private|high income'
                                  || '|total return|\besg\b|climate|green|sustainable|convertible'
                                  || '|preferred|floating|\bfloater|loan|\bclo\b|leverag|inverse'
                                  || '|\bbear\b|\b[23]x\b|ultra|0.1 year|0.3 month')
        and not regexp_matches(coalesce(sub_asset_class, ''),
                               '(?i)municipal|convertible|bank loan|flexible|securitized')
                                                                       as bond_ok,
        not regexp_matches(name, '(?i)miner|mining|silver|platinum|palladium|leverag|inverse|\b[23]x\b'
                              || '|ultra|covered|premium|income|buffer|futures|strategy|equity'
                              || '|royalt|hedged|option')              as gold_ok,
        regexp_matches(name, '(?i)commodity|gsci')
        and not regexp_matches(name, '(?i)silver|gold|platinum|palladium|\boil\b|crude|\bgas\b|energy'
                                  || '|agricult|grain|corn|wheat|soy|sugar|coffee|cocoa|cotton'
                                  || '|livestock|copper|metal|lithium|uranium|carbon|natural resources'
                                  || '|miner|mining|equity|producers|leverag|inverse|\b[23]x\b|ultra'
                                  || '|covered|premium|income|buffer')
                                                                       as commodity_ok,
        not regexp_matches(name, '(?i)income|covered|futures|strategy|leverag|inverse|\b[23]x\b|ultra'
                              || '|buffer|option|hedge|ether')         as bitcoin_ok
    from funds

), classified as (

    select
        *,
        case
            when role = 'equity' and asset_class not in ({{ sql_list('fund_asset_classes') }})
                                                                              then 'asset_class'
            when security_key is null                                         then 'no_prices'
            -- A null flag (no name) is not a pass.
            when role = 'bond' and not coalesce(bond_ok, false)               then 'bond_excluded'
            when role = 'gold' and not coalesce(gold_ok, false)               then 'gold_excluded'
            when role = 'commodity' and not coalesce(commodity_ok, false)     then 'commodity_excluded'
            when role = 'bitcoin' and not coalesce(bitcoin_ok, false)         then 'bitcoin_excluded'
            -- The name rules below are those of the equity and cash roles only.
            when role not in ('equity', 'cash')                               then null
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
    from kinds

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
