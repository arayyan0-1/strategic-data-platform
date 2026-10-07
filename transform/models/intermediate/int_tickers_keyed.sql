{# The ticker reference per date with the security key. The one definition of the key rule. #}

with keyed as (

    -- A link gives the rows of a new FIGI the first key of the security.
    select
        e.* exclude (name_word, episode_cik, episode_key, key_rule),
        coalesce(l.root_key, e.episode_key) as security_key,
        e.key_rule,
        e.episode_key
    from {{ ref('int_ticker_episodes') }} e
    left join {{ ref('int_security_links') }} l
        on l.new_key = e.episode_key

)

select
    *,
    -- The vendor type is null on some dates of a known name, so take the security's last known
    -- type (earlier dates only). Two lines can share a date: order by the shorter ticker, as in
    -- int_security_lines, so the fill is the same in every build.
    coalesce(type, last_value(type ignore nulls) over (
        partition by security_key order by date, length(ticker), ticker
        rows between unbounded preceding and current row
    )) as type_filled
from keyed
