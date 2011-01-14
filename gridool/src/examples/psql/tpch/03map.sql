select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as C2,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem 
where
	(lineitem._hidden & 2) = 2	-- partition by orderkey
	and (orders._hidden & 2) = 2	-- partition by orderkey
	and (customer._hidden & 18) <> 0 -- partition by orderkey or custkey
	and c_mktsegment = 'BUILDING'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-15'
	and l_shipdate > date '1995-03-15'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority

