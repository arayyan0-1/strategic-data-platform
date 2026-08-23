{% macro lake(dataset, key='date') -%}
    read_parquet(
        '{{ var("data_root") }}/raw/{{ dataset }}/{{ key }}=*/data.parquet',
        hive_partitioning = true
    )
{%- endmacro %}


{#
  The pull that a corporate action model reads.
  A current-state dataset has no history before the first pull, so a study of the
  historical window cannot use snapshot(as_of). It must name one pull and state
  which one. See docs/decisions/0011.
#}
{% macro ca_pull_date(dataset) -%}
    {%- set policy = var('ca_pull_policy') -%}
    {%- if policy == 'earliest' -%}
        (select min(pull_date) from {{ lake(dataset, 'pull_date') }})
    {%- elif policy == 'latest' -%}
        (select max(pull_date) from {{ lake(dataset, 'pull_date') }})
    {%- else -%}
        (select max(pull_date) from {{ lake(dataset, 'pull_date') }}
          where pull_date <= date '{{ policy }}')
    {%- endif -%}
{%- endmacro %}


{#
  The snapshot of the policy pull, replayed from the log.

  raw/ stores splits and dividends as an append-only snapshot log. Each
  partition holds the change of one pull: op = 'add' when a row entered the
  snapshot, op = 'close' when it left. The newest log row for a row_hash
  decides. This macro is the SQL twin of dal.snapshot() and it must give the
  same rows. See docs/decisions/0015.
#}
{% macro ca_snapshot(dataset) -%}
    (
        select * exclude (op, row_hash, _rn)
        from (
            select *, row_number() over (
                partition by row_hash order by pull_date desc) as _rn
            from {{ lake(dataset, 'pull_date') }}
            where pull_date <= {{ ca_pull_date(dataset) }}
        )
        where _rn = 1 and op = 'add'
    )
{%- endmacro %}
