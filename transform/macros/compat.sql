-- macros/compat.sql — alias shims for legacy model SQL

{% macro safe_numeric(expr,p=38,s=6) -%} try_cast({{ expr }} as decimal({{p}},{{s}})) {%- endmacro %}
{% macro safe_timestamp(expr) -%} try_cast({{ expr }} as timestamp) {%- endmacro %}
{% macro safe_date(expr) -%}      try_cast({{ expr }} as date)      {%- endmacro %}
{% macro safe_text(expr) -%}      nullif(trim(cast({{ expr }} as varchar)),"") {%- endmacro %}

{% macro std_session(expr) -%}     {{ normalize_session_code(expr) }}   {%- endmacro %}
{% macro std_compound(expr) -%}    {{ normalize_compound(expr) }}       {%- endmacro %}
{% macro std_slugify(expr) -%}     {{ slugify_event(expr) }}            {%- endmacro %}
{% macro std_upper_trim(expr) -%}  {{ normalize_text(expr, casing='upper') }} {%- endmacro %}

{% macro slugify_gp(expr) -%}          {{ slugify_event(expr) }} {%- endmacro %}
{% macro slugify_grand_prix(expr) -%}  {{ slugify_event(expr) }} {%- endmacro %}

{% macro normalize_tyre_compound(expr) -%} {{ normalize_compound(expr) }} {%- endmacro %}
{% macro normalize_grand_prix(expr) -%}    {{ slugify_event(expr) }}     {%- endmacro %}
{% macro normalize_event(expr) -%}         {{ slugify_event(expr) }}     {%- endmacro %}
{% macro normalize_gp(expr) -%}            {{ slugify_event(expr) }}     {%- endmacro %}
