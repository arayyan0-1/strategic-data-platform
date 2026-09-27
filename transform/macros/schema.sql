{# The schema of a model is its folder schema as written (staging, research), not
   the default schema joined to it (main_staging). #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}
