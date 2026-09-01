{% macro zero_if_null(coluna) %}
    coalesce({{ coluna }}, 0)
{% endmacro %}