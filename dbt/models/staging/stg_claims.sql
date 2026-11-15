-- One row per claim, with the flags and ratios the marts share.
select
    claim_id,
    year_of_loss,
    date_of_loss,
    state,
    county_fips,
    rated_flood_zone,
    flood_event,
    is_primary_residence,
    building_property_value,
    building_damage_amount,
    contents_damage_amount,
    building_coverage,
    contents_coverage,
    paid_building,
    paid_contents,
    paid_icc,
    total_paid,
    total_paid > 0 as was_paid,
    coalesce(building_damage_amount, 0) + coalesce(contents_damage_amount, 0) as total_damage,
    -- Share of the building's value that was damaged. Null when the value is missing or zero.
    case
        when building_property_value > 0 then building_damage_amount / building_property_value
    end as building_damage_ratio
from {{ source('curated', 'nfip_claims') }}
where state is not null
