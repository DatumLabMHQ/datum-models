{# Schema per product, isolated per environment.
   prod  -> sui, morpho, rwa               (the tables products read)
   dev   -> dev_sui, dev_morpho ...        (a developer's local builds)
   ci    -> ci_<run>_sui ...               (pull-request builds, dropped afterwards)
   Raw tables always live in the prod schema and are read by every environment. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set base = (custom_schema_name | trim) if custom_schema_name is not none else target.schema -%}
  {%- if target.name == 'prod' -%}
    {{ base }}
  {%- elif target.name == 'ci' -%}
    ci_{{ env_var('CI_RUN_ID', 'local') }}_{{ base }}
  {%- else -%}
    {{ target.name }}_{{ base }}
  {%- endif -%}
{%- endmacro %}
