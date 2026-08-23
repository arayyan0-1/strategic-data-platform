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
