{{
    config(
        materialized = 'table'
    )
}}

select
    loadsmart_id,
    shipper_nm
from {{ ref('stg_ae_data') }}
where shipper_nm is not null