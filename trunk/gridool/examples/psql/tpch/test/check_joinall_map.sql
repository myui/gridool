select 
    count(*) as C1
from
    part,
    partsupp,
    supplier,
    nation,
    lineitem,
    customer,
    region,
    orders
where
	part partitioned by (p_partkey)
	and partsupp partitioned by (ps_partkey,ps_suppkey)
	and supplier partitioned by (s_suppkey,s_nationkey)
	and nation partitioned by (n_nationkey,n_regionkey)
	and lineitem partitioned by (l_orderkey,l_partkey,l_suppkey)	
	and customer partitioned by (c_custkey,c_nationkey)
	and region partitioned by (r_regionkey)
	and orders partitioned by (o_orderkey,o_custkey)
    and p_partkey = ps_partkey
    and ps_partkey = l_partkey
    and ps_suppkey = s_suppkey
    and ps_suppkey = l_suppkey
    and s_nationkey = n_nationkey
    and n_nationkey = c_nationkey
    and l_orderkey = o_orderkey
    and c_custkey = o_custkey
    and r_regionkey = n_regionkey
