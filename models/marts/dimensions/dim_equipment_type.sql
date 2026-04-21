{{
    config(
        materialized = 'table'
    )
}}

select
    loadsmart_id,
    equipment_type
from {{ ref('stg_ae_data') }}
where equipment_type is not null