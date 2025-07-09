-- e.g. => dbt run-operation drop_dev_schema --args '{schema_name: dbt_mfvitor}'
{% macro drop_dev_schema(schema_name) %}
  {% set drop_schema_sql %}
    DROP SCHEMA IF EXISTS {{ schema_name }};
  {% endset %}
  
  {{ log("Starting schema drop for: " ~ schema_name, info=True) }}
  {% do run_query(drop_schema_sql) %}
  {{ log("Schema " ~ schema_name ~ " dropped successfully.", info=True) }}
{% endmacro %}