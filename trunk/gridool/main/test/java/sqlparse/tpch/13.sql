select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey)
		from
			customer 
				left outer join 
			orders 
				on c_custkey = o_custkey
				and o_comment not like '%special%requests%'
		where
			customer partitioned by (c_custkey)
			and orders partitioned by (o_custkey)
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc;
