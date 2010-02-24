select 
    count(1) as C1
from
    lineitem,
    partsupp,
    part,
    supplier,
    orders,
    customer,
    nation,
    region
where
	--part partitioned by (p_partkey)
	--and partsupp partitioned by (ps_partkey,ps_suppkey)
	--and supplier partitioned by (s_suppkey,s_nationkey)
	--and nation partitioned by (n_nationkey,n_regionkey)
	--and lineitem partitioned by (l_orderkey,l_partkey,l_suppkey)	
	--and customer partitioned by (c_custkey,c_nationkey)
	--and region partitioned by (r_regionkey)
	--and orders partitioned by (o_orderkey,o_custkey)
    l_partkey = ps_partkey
    and l_suppkey = ps_suppkey
    and l_partkey = p_partkey
    and l_suppkey = s_suppkey    
    and l_orderkey = o_orderkey
    and ps_partkey = p_partkey
    and ps_suppkey = s_suppkey
    and o_custkey = c_custkey
    and s_nationkey = n_nationkey
    and c_nationkey = n_nationkey
    and n_regionkey = r_regionkey
