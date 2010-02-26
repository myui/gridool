select
	nation,
	o_year,
	sum(C3) as sum_profit
from
	<src>
group by
	nation,
	o_year
order by
	nation,
	o_year desc;