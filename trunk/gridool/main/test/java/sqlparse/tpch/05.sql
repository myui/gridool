select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer partitioned by c_custkey and c_nationkey,
	orders partitioned by o_custkey,
	lineitem partitioned by l_orderkey and l_suppkey,
	supplier partitioned by s_suppkey and s_nationkey,
	nation partitioned by n_nationkey and n_regionkey,
	region partitioned by r_regionkey
where
	c_custkey = o_custkey
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
order by
	revenue desc;
