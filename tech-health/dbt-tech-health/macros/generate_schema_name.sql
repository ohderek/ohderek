{#
generate_schema_name

Overrides the default dbt schema generation to support environment-aware
schema naming. Matches the forge-tech-health Catalyst framework convention.

Logic:
  - If no custom schema: use target.schema (developer personal schema)
  - If target contains 'sandbox_': prefix custom schema with the sandbox owner
  - Otherwise: use the custom schema directly (prod/staging)
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- elif "sandbox_" in default_schema -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
