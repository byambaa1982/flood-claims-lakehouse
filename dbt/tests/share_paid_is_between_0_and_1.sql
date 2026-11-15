select state, year_of_loss, share_paid
from {{ ref('mart_loss_by_state_year') }}
where share_paid < 0 or share_paid > 1
