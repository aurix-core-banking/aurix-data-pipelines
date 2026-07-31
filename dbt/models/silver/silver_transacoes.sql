with transacoes as (
    select * from {{ ref('stg_transacoes') }}
),

enriched as (
    select
        t.*,
        case
            when t.valor > 50000 then 'ALTO'
            when t.valor > 10000 then 'MEDIO'
            else 'BAIXO'
        end as faixa_valor,
        extract(hour from t.data_transacao) as hora_transacao,
        extract(dow from t.data_transacao) as dia_semana
    from transacoes t
)

select * from enriched