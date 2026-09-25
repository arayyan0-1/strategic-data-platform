{# A list var as SQL literals. A string var would split into its characters and
   build a wrong filter with no error, so anything but a list stops the build. #}
{% macro sql_list(name) -%}
    {%- set values = var(name) -%}
    {%- if values is string or values is mapping or values is not iterable or not values -%}
        {{ exceptions.raise_compiler_error(
            "The var " ~ name ~ " must be a non-empty list, for example [\"CS\"]. It is "
            ~ values ~ ".") }}
    {%- endif -%}
    {%- for v in values -%}
        '{{ v }}'{{ ", " if not loop.last }}
    {%- endfor -%}
{%- endmacro %}
