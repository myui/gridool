select
	n_name,
	sum(C2) as revenue
from
	<src>
group by
	n_name
order by
	revenue desc;
