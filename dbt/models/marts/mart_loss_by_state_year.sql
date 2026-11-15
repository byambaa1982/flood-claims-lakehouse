-- Claims and dollars by state and year of loss.
select
    state,
    year_of_loss,
    count(*) as claims,
    count_if(was_paid) as claims_paid,
    cast(count_if(was_paid) as double) / count(*) as share_paid,
    sum(total_paid) as total_paid,
    sum(total_damage) as total_damage,
    avg(case when was_paid then total_paid end) as avg_paid_per_paid_claim,
    approx_percentile(case when was_paid then total_paid end, 0.5) as median_paid_per_paid_claim
from {{ ref('stg_claims') }}
group by 1, 2
