with transacoes as (
    select * from {{ ref('silver_transacoes') }}
),

diario as (
    select
        date_trunc('day', data_transacao) as data,
        tipo_transacao,
        count(*) as total_transacoes,
        sum(valor) as volume_total,
        avg(valor) as ticket_medio,
        count(distinct conta_origem_id) as contas_unicas
    from transacoes
    where status = 'PROCESSADA'
    group by 1, 2
)

select * from diario