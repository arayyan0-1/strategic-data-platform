{#
  The ticker reference per date, with the security key. The universe, the security
  lines and the price adjustment all read it, so the key rule has one definition.
#}

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
    -- The vendor type is null on some dates of a known name. Take the last known type
    -- of the security. The fill reads earlier dates only.
    coalesce(type, last_value(type ignore nulls) over (
        partition by security_key order by date
        rows between unbounded preceding and current row
    )) as type_filled
from keyed
