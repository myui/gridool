select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as C2
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	customer partitioned by (c_custkey, c_nationkey)
	and orders partitioned by (o_custkey)
	and lineitem partitioned by (l_orderkey, l_suppkey)
	and supplier partitioned by (s_suppkey, s_nationkey)
	and nation partitioned by (n_nationkey, n_regionkey)
	and region partitioned by (r_regionkey)
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and o_orderdate >= date '1994-01-01'
	and o_orderdate < date '1994-01-01' + interval '1' year
group by
	n_name