{% macro classifica_faixa_valor(coluna, limite_alto=50000, limite_medio=10000) %}
    case
        when {{ coluna }} > {{ limite_alto }}::numeric then 'ALTO'
        when {{ coluna }} > {{ limite_medio }}::numeric then 'MEDIO'
        else 'BAIXO'
    end
{% endmacro %}