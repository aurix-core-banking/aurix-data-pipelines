{% macro documento_pessoa(coluna_cpf, coluna_cnpj) %}
    case
        when {{ coluna_cpf }} is not null and length(trim({{ coluna_cpf }})) > 0 then {{ coluna_cpf }}
        else {{ coluna_cnpj }}
    end
{% endmacro %}