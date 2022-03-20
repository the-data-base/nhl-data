{#
Override the default schema behaviour
If a custom schema is provided, a model's schema name should match the custom schema, rather than being concatenated to the target schema.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if target.name == 'prod' -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- else -%}
        {{ default_schema }}

    {%- endif -%}

{%- endmacro %}
