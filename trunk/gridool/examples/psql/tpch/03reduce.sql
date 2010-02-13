select
	l_orderkey,
	sum(C2) as revenue,
	o_orderdate,
	o_shippriority
from
	<src>
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
limit 10;
