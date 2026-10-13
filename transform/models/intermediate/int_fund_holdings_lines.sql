{# The long common-equity lines of each N-PORT report, with the security_key of the line. The one
   definition of how a line maps to a security. A line's code is its CUSIP, else its ISIN (iShares
   states no CUSIP for a name domiciled outside the US).

   1. figi: OpenFIGI gives the code a share-class FIGI (and a composite FIGI), which is the episode
      key of the security in int_tickers_keyed; a link to a later key then gives the first key of
      the chain. This is the route of every security the vendor keys by FIGI, and the share-class
      FIGI outlives a US delisting.
   2. ticker: the vendor has no FIGI for about 8% of securities and keys them by CIK and ticker
      (Linde, Accenture, Medtronic, Chubb: US-listed, Irish or Swiss domiciled), and after a
      reorganization its FIGI can differ from OpenFIGI's (CRH, FTAI). For a line the first route
      misses, the US ticker OpenFIGI gives is matched to a ticker of the reference, and only when
      the first word of the issuer name agrees on both sides. OpenFIGI states the ticker of today,
      so without that name check a reused ticker would put the holding on another company.

   A line neither route reaches, or whose security has no bar, keeps a null security_key. #}

with lines as (

    select fund, report_date, filed, coalesce(cusip, isin) as code, name, val_usd
    from {{ ref('stg_sec__nport') }}
    where asset_cat = 'EC' and payoff = 'Long' and val_usd is not null

), figis as (

    select coalesce(cusip, isin) as code, share_class_figi, composite_figi, ticker
    from {{ ref('stg_openfigi__cusips') }}
    where share_class_figi is not null

), priced as (

    -- Only a security with a bar can carry a signal, so a key seen in the ticker reference alone
    -- stays unmapped.
    select distinct security_key from {{ ref('int_security_lines') }}

), keys as (

    select episode_key, security_key
    from {{ ref('int_tickers_keyed') }}
    semi join priced using (security_key)
    group by episode_key, security_key

), by_figi as (

    select
        l.*,
        f.ticker as figi_ticker,
        coalesce(by_class.security_key, by_composite.security_key) as security_key
    from lines l
    left join figis f using (code)
    left join keys by_class on by_class.episode_key = f.share_class_figi
    left join keys by_composite on by_composite.episode_key = f.composite_figi

), named as (

    select
        ticker,
        security_key,
        lower(regexp_extract(name, '[A-Za-z0-9]+')) as name_word,
        max(date) as last_date
    from {{ ref('int_tickers_keyed') }}
    semi join priced using (security_key)
    group by ticker, security_key, name_word

), asked as (

    select distinct code, figi_ticker, lower(regexp_extract(name, '[A-Za-z0-9]+')) as name_word
    from by_figi
    where security_key is null and figi_ticker is not null

), by_ticker as (

    select a.code, c.security_key
    from asked a
    join named c on c.ticker = a.figi_ticker and c.name_word = a.name_word
    qualify row_number() over (partition by a.code order by c.last_date desc, c.security_key) = 1

)

select
    l.fund,
    l.report_date,
    l.filed,
    l.code,
    l.val_usd,
    coalesce(l.security_key, t.security_key) as security_key,
    case when l.security_key is not null then 'figi'
         when t.security_key is not null then 'ticker' end as matched_on
from by_figi l
left join by_ticker t on t.code = l.code and l.security_key is null
