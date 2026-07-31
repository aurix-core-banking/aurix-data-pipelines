with source as (
    select * from {{ source('aurix', 'clientes') }}
),

renamed as (
    select
        id as cliente_id,
        tipo_pessoa,
        cpf,
        nome,
        cnpj,
        nome_razao_social,
        email,
        telefone,
        data_nascimento,
        cidade,
        estado,
        cep,
        status,
        data_criacao,
        data_atualizacao
    from source
)

select * from renamed