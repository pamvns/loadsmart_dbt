{{
    config(
        materialized = 'table'
    )
}}

select
    loadsmart_id,
    carrier_nm,
    carrier_rating_fl,
    is_vip_carrier,
    carrier_dropped_us_count,
    sourcing_channel
from {{ ref('stg_ae_data') }}
where carrier_nm is not null