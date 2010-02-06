select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	customer,
	orders,
	lineitem 
where
	customer partitioned by (c_custkey)
	and orders partitioned by (o_orderkey, o_custkey)
	and lineitem partitioned by (l_orderkey)
	and o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		where
			lineitem partitioned by (l_orderkey)
		group by
			l_orderkey 
		having
			sum(l_quantity) > 300
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
limit 100;
