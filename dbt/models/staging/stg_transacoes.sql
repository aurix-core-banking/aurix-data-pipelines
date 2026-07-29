{{ config(materialized='view', schema='aurix') }}
select
    id,
    tenant_id,
    conta_origem_id,
    conta_destino_id,
    tipo_transacao,
    valor,
    descricao,
    status,
    codigo_transacao,
    data_transacao,
    data_processamento,
    data_criacao
from {{ source('aurix', 'transacoes') }}
