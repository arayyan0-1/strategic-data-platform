{# Read a source that may have no file yet, such as a new dataset or a fresh clone: with no
   file it reads as an empty table with the given columns instead of failing the build. #}
{% macro from_lake(src, identifier, columns, files='date=*/data.parquet') -%}
    {%- set found = 1 -%}
    {%- if execute -%}
        {%- set q -%}
            select count(*) from glob('{{ env_var('SDP_DATA_ROOT') }}/raw/{{ identifier }}/{{ files }}')
        {%- endset -%}
        {%- set found = run_query(q).columns[0].values()[0] -%}
    {%- endif -%}
    {%- if found > 0 -%}
        {{ src }}
    {%- else -%}
        (select {% for name, type in columns.items() -%}
            cast(null as {{ type }}) as {{ name }}{{ ", " if not loop.last }}
        {%- endfor %} where false)
    {%- endif -%}
{%- endmacro %}
