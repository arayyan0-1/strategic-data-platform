{% macro lake(dataset, key='date') -%}
    read_parquet(
        '{{ var("data_root") }}/raw/{{ dataset }}/{{ key }}=*/data.parquet',
        hive_partitioning = true
    )
{%- endmacro %}


{#
  The corporate action table.

  raw/ holds one file for each of splits and dividends, replaced by every
  pull, with no date in the path. There is therefore no pull to choose and no
  policy var. This macro is the SQL twin of dal.current().
  See docs/decisions/0016.
#}
{% macro ca_table(dataset) -%}
    read_parquet('{{ var("data_root") }}/raw/{{ dataset }}/data.parquet')
{%- endmacro %}
