select
	l_returnflag,
	l_linestatus,
	sum(C3) as sum_qty,
	sum(C4) as sum_base_price,
	sum(C5) as sum_disc_price,
	sum(C6) as sum_charge,
	sum(C7)/cast(sum(C10) as double) as avg_qty,
	sum(C8)/cast(sum(C10) as double) as avg_price,
	sum(C9)/cast(sum(C10) as double) as avg_disc,
	count(C10) as count_order
from
	<src>
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
