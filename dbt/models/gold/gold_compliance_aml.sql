{{ config(materialized='table', schema='aurix') }}

-- Indicadores PLD/AML por cliente (COAF): volume movimentado, transacoes acima de
-- R$ 50 mil, ticket medio e nivel de atencao para reporte regulatorio.
with transacoes as (
    select * from {{ ref('silver_transacoes') }}
    where status = 'PROCESSADA'
),

contas as (
    select id, cliente_id from {{ ref('stg_contas') }}
),

transacoes_clientes as (
    select
        c.cliente_id,
        t.id,
        t.valor,
        t.data_transacao
    from transacoes t
    join contas c on t.conta_origem_id = c.id
),

agregado_cliente as (
    select
        cliente_id,
        count(*) as total_transacoes_30d,
        sum(valor) as volume_30d,
        avg(valor) as ticket_medio_30d,
        max(valor) as maior_transacao_30d,
        sum(case when valor > 50000 then 1 else 0 end) as transacoes_acima_50k_30d,
        count(distinct date(data_transacao)) as dias_com_movimentacao_30d,
        max(data_transacao) as data_ultima_transacao
    from transacoes_clientes
    where {{ filtro_ultimos_dias('data_transacao', 30) }}
    group by cliente_id
),

clientes as (
    select * from {{ ref('silver_clientes') }}
),

final as (
    select
        a.cliente_id,
        cl.tipo_pessoa,
        {{ documento_pessoa('cl.cpf', 'cl.cnpj') }} as documento,
        cl.nome,
        a.total_transacoes_30d,
        a.volume_30d,
        a.ticket_medio_30d,
        a.maior_transacao_30d,
        a.transacoes_acima_50k_30d,
        a.dias_com_movimentacao_30d,
        a.data_ultima_transacao,
        case
            when a.volume_30d > 100000 or a.transacoes_acima_50k_30d >= 3 then 'ALTA'
            when a.volume_30d > 50000 or a.transacoes_acima_50k_30d >= 1 then 'MEDIA'
            else 'BAIXA'
        end as nivel_atencao_compliance,
        current_timestamp as data_referencia
    from agregado_cliente a
    left join clientes cl on a.cliente_id = cl.cliente_id
    where a.volume_30d > 50000 or a.transacoes_acima_50k_30d >= 1
)

select * from final