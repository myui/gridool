select 
	o_orderpriority,
	sum(C2) as order_count
from
	<src>
group by
	o_orderpriority
order by
	o_orderpriority;
