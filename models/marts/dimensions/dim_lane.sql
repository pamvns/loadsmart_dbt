{{
    config(
        materialized = 'table'
    )
}}

select
    loadsmart_id,
    lane,
    origin_city,
    origin_state,
    destination_city,
    destination_state,
    concat(origin_state, ' -> ', destination_state) as state_lane
from {{ ref('stg_ae_data') }}
where lane is not null