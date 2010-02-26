select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity) as C6
from
	customer,
	orders,
	lineitem 
where
	lineitem._hidden & 2 = 2
	and orders._hidden & 2 = 2
	and customer._hidden & 18 <> 0
	and o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		where
			lineitem._hidden & 2  = 2
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
