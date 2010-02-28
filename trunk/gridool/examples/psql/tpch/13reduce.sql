select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey, 
			sum(C2)
		from 
			<src>
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc;
