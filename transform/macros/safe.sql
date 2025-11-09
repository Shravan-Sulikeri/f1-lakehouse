{% macro bool_to_int(expr) -%}
  CASE WHEN {{ expr }} THEN 1 ELSE 0 END
{%- endmacro %}
