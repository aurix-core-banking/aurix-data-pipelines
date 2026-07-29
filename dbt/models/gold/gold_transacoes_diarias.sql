{{ config(materialized='table', schema='aurix') }}
select
    date(data_transacao) as data,
    tipo_transacao,
    count(*) as quantidade,
    sum(valor) as volume_total
from {{ ref('stg_transacoes') }}
where data_transacao is not null
group by date(data_transacao), tipo_transacao
order by data desc, tipo_transacao
