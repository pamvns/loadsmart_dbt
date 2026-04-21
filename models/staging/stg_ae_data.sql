with source as (
    select *
    from {{ ref('2026_data_challenge_ae_data') }}
),

-- 4 loadsmart_ids appear more than once in the source; keep one row each.
-- No business rule available, so we pick deterministically by loadsmart_id.
deduplicated as (
    select *
    from source
    qualify row_number() over (partition by loadsmart_id order by loadsmart_id) = 1
),

renamed as (
    select
        -- identifiers
        cast(loadsmart_id as string) as loadsmart_id,

        -- lane (raw + parsed components)
        lane,
        regexp_extract(lane, r'^(.+),[A-Z]{2} ->') as origin_city,
        regexp_extract(lane, r'^.+,([A-Z]{2}) ->') as origin_state,
        regexp_extract(lane, r'-> (.+),[A-Z]{2}$') as destination_city,
        regexp_extract(lane, r'-> .+,([A-Z]{2})$') as destination_state,

        -- timestamps  (_ts = datetime)
        parse_datetime('%m/%d/%Y %H:%M', quote_date) as quoted_ts,
        parse_datetime('%m/%d/%Y %H:%M', book_date) as booked_ts,
        parse_datetime('%m/%d/%Y %H:%M', source_date) as sourced_ts,
        parse_datetime('%m/%d/%Y %H:%M', pickup_date) as pickup_ts,
        parse_datetime('%m/%d/%Y %H:%M', delivery_date) as delivered_ts,
        parse_datetime('%m/%d/%Y %H:%M', pickup_appointment_time) as pickup_appointment_ts,
        parse_datetime('%m/%d/%Y %H:%M', delivery_appointment_time) as delivery_appointment_ts,

        -- financials  (_nm = numeric, _fl = float)
        cast(book_price as numeric) as book_price_nm,
        cast(source_price as numeric) as source_price_nm,
        cast(pnl as numeric) as pnl_nm,
        cast(mileage as numeric) as mileage_nm,

        -- equipment
        equipment_type,

        -- carrier attributes
        carrier_name as carrier_nm,
        cast(carrier_rating as float64) as carrier_rating_fl,
        cast(vip_carrier as bool) as is_vip_carrier,
        cast(carrier_dropped_us_count as int64) as carrier_dropped_us_count,
        nullif(sourcing_channel, '') as sourcing_channel,

        -- shipper
        shipper_name as shipper_nm,

        -- carrier performance
        cast(carrier_on_time_to_pickup as bool) as is_carrier_on_time_to_pickup,
        cast(carrier_on_time_to_delivery as bool) as is_carrier_on_time_to_delivery,
        cast(carrier_on_time_overall as bool) as is_carrier_on_time_overall,

        -- tracking methods
        -- NOTE: source CSV has 'has_mobile_app_tracking' duplicated (columns 24 and 25);
        -- the second occurrence carries no distinct signal and is dropped here.
        cast(has_mobile_app_tracking as bool) as has_mobile_app_tracking,
        cast(has_macropoint_tracking as bool) as has_macropoint_tracking,
        cast(has_edi_tracking as bool) as has_edi_tracking,

        -- load flags
        cast(contracted_load as bool) as is_contracted_load,
        cast(load_booked_autonomously as bool) as is_booked_autonomously,
        cast(load_sourced_autonomously as bool) as is_sourced_autonomously,
        cast(load_was_cancelled as bool) as was_cancelled
    from deduplicated
)

select *
from renamed
