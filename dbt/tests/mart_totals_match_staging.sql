-- The state and year mart must add up to the staging model: same claims, same dollars.
-- Returns a row, and so fails, when they differ.
with mart as (
    select sum(claims) as claims, sum(total_paid) as total_paid
    from {{ ref('mart_loss_by_state_year') }}
),
staging as (
    select count(*) as claims, sum(total_paid) as total_paid
    from {{ ref('stg_claims') }}
)
select mart.claims, staging.claims as staging_claims, mart.total_paid, staging.total_paid as staging_total_paid
from mart cross join staging
where mart.claims <> staging.claims
   or abs(mart.total_paid - staging.total_paid) > 1
