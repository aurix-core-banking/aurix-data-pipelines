with clientes as (
    select * from {{ ref('silver_clientes') }}
),

transacoes_agg as (
    select
        c.cliente_id,
        count(*) as total_transacoes_30d,
        sum(t.valor) as volume_30d,
        avg(t.valor) as ticket_medio_30d,
        max(t.valor) as maior_transacao_30d,
        count(distinct t.tipo_transacao) as tipos_utilizados
    from {{ ref('silver_transacoes') }} t
    join clientes c on t.conta_origem_id in (
        select id from {{ ref('stg_contas') }} where cliente_id = c.cliente_id
    )
    where {{ filtro_ultimos_dias('t.data_transacao', 30) }}
    group by c.cliente_id
),

risco as (
    select
        c.*,
        {{ zero_if_null('ta.total_transacoes_30d') }} as total_transacoes_30d,
        {{ zero_if_null('ta.volume_30d') }} as volume_30d,
        ta.ticket_medio_30d,
        ta.maior_transacao_30d,
        ta.tipos_utilizados,
        case
            when coalesce(ta.volume_30d, 0) > 100000 then 'ALTO'
            when coalesce(ta.volume_30d, 0) > 50000 then 'MEDIO'
            else 'BAIXO'
        end as nivel_risco_volume
    from clientes c
    left join transacoes_agg ta on c.cliente_id = ta.cliente_id
)

select * from risco