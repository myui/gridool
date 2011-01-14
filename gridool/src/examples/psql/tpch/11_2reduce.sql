select
	ps_partkey,
	sum(C2) as value
from
	<src>
group by
	ps_partkey 
order by
	value desc;