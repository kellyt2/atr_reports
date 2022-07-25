select 
to_char(ld.settled_date_time,'yyyy-mm-dd') as day_settled,
ld.brand,
ld.sport_name,
sum(ld.vol_gbp) as volume,
sum(ld.rev_gbp) as revenue
from trading_shared.vw_apb_rep_test ld

where ld.settled_date_time > '{{start_date}}'
and ld.settled_date_time < '{{end_date}}'

group by 1,2,3
