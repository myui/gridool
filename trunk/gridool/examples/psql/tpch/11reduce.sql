select
	ps_partkey,
	sum(C2) as value
from
	<src>
order by
	value desc;