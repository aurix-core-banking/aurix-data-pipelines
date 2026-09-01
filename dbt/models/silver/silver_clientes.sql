with clientes as (
    select * from {{ ref('stg_clientes') }}
),

contas_agg as (
    select
        cliente_id,
        count(*) as total_contas,
        sum(saldo) as saldo_total,
        max(data_abertura) as ultima_conta_abertura
    from {{ ref('stg_contas') }}
    group by cliente_id
),

enriched as (
    select
        c.*,
        {{ zero_if_null('ca.total_contas') }} as total_contas,
        {{ zero_if_null('ca.saldo_total') }} as saldo_total,
        ca.ultima_conta_abertura
    from clientes c
    left join contas_agg ca on c.cliente_id = ca.cliente_id
)

select * from enriched