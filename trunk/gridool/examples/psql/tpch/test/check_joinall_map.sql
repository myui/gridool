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
	(lineitem._hidden & 1 = 1)
	and (customer._hidden & 1 = 1)
    and l_partkey = ps_partkey
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
