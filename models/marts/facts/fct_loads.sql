{{
    config(
        materialized = 'table'
    )
}}

with stg as (
    select *
    from {{ ref('stg_ae_data') }}
),

final as (
    select
        -- natural key
        loadsmart_id,

        -- date grains  (_dt = date)
        date(quoted_ts) as quote_dt,
        date(booked_ts) as book_dt,
        date(pickup_ts) as pickup_dt,
        date(delivered_ts) as delivery_dt,

        -- full timestamps
        quoted_ts,
        booked_ts,
        sourced_ts,
        pickup_ts,
        delivered_ts,
        pickup_appointment_ts,
        delivery_appointment_ts,

        -- lead-time metrics  (_days = days)
        date_diff(date(booked_ts), date(quoted_ts), day) as quote_to_book_days,
        date_diff(date(sourced_ts), date(booked_ts), day) as book_to_source_days,
        date_diff(date(pickup_ts), date(booked_ts), day) as book_to_pickup_days,
        date_diff(date(delivered_ts), date(pickup_ts), day) as transit_days,

        -- financial metrics  (_nm = numeric)
        book_price_nm,
        source_price_nm,
        pnl_nm,
        mileage_nm,
        safe_divide(book_price_nm, nullif(mileage_nm, 0)) as book_price_per_mile_nm,
        safe_divide(source_price_nm, nullif(mileage_nm, 0)) as source_price_per_mile_nm,

        -- sourcing
        sourcing_channel,

        -- carrier performance
        is_carrier_on_time_to_pickup,
        is_carrier_on_time_to_delivery,
        is_carrier_on_time_overall,

        -- tracking methods
        has_mobile_app_tracking,
        has_macropoint_tracking,
        has_edi_tracking,

        -- load flags
        is_contracted_load,
        is_booked_autonomously,
        is_sourced_autonomously,
        was_cancelled
    from stg
)

select *
from final
