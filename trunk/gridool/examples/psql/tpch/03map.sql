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
	customer partitioned by (c_custkey)
	and orders partitioned by (o_custkey, o_orderkey)
	and lineitem partitioned by (l_orderkey)
	and c_mktsegment = 'BUILDING'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-15'
	and l_shipdate > date '1995-03-15'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority

