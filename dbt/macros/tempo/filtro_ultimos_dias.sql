{% macro filtro_ultimos_dias(coluna, dias=30) %}
    {{ coluna }} >= current_date - interval '{{ dias }} days'
{% endmacro %}