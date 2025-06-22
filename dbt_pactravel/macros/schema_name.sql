{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ 'public' }}  {# Fallback for seeds/tests without explicit schema #}
    {%- else -%}
        {{ custom_schema_name | trim }}  {# Uses exactly what you specify #}
    {%- endif %}
{%- endmacro %}