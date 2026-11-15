-- One row per named flood event.
select
    flood_event,
    min(date_of_loss) as first_loss_date,
    max(date_of_loss) as last_loss_date,
    count(distinct state) as states_affected,
    count(*) as claims,
    sum(total_paid) as total_paid,
    avg(case when was_paid then total_paid end) as avg_paid_per_paid_claim
from {{ ref('stg_claims') }}
where flood_event is not null
group by 1
