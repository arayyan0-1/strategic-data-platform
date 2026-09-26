{% macro lake(dataset, key='date') -%}
    read_parquet(
        '{{ var("data_root") }}/raw/{{ dataset }}/{{ key }}=*/data.parquet',
        hive_partitioning = true
    )
{%- endmacro %}


{# The corporate action table: one file per dataset, replaced by each pull.
   The SQL twin of dal.current(). #}
{% macro ca_table(dataset) -%}
    read_parquet('{{ var("data_root") }}/raw/{{ dataset }}/data.parquet')
{%- endmacro %}


{# A current-state table that is not a corporate action table. #}
{% macro current_table(dataset) -%}
    read_parquet('{{ var("data_root") }}/raw/{{ dataset }}/data.parquet')
{%- endmacro %}
