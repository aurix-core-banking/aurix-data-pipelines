with transacoes as (
    select * from {{ ref('stg_transacoes') }}
),

enriched as (
    select
        t.*,
        {{ classifica_faixa_valor('t.valor') }} as faixa_valor,
        extract(hour from t.data_transacao) as hora_transacao,
        extract(dow from t.data_transacao) as dia_semana
    from transacoes t
)

select * from enriched