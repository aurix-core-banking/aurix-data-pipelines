with contas as (
    select * from {{ ref('stg_contas') }}
),

resumo as (
    select
        date_trunc('day', data_atualizacao) as data,
        tipo_conta,
        status,
        count(*) as total_contas,
        sum(saldo) as saldo_total,
        avg(saldo) as saldo_medio,
        sum(limite_credito) as limite_total,
        sum(limite_utilizado) as limite_utilizado_total
    from contas
    group by 1, 2, 3
)

select * from resumo