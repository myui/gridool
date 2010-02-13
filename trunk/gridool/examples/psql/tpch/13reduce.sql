select
	c_count,
	count(C2) as custdist
from
	<src>
group by
	c_count
order by
	custdist desc,
	c_count desc;
