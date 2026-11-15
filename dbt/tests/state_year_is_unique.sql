select state, year_of_loss, count(*) as n
from {{ ref('mart_loss_by_state_year') }}
group by 1, 2
having count(*) > 1
