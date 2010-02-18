select 
    count(1)
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
    p_partkey = ps_partkey
    and ps_partkey = l_partkey
    and ps_suppkey = s_suppkey
    and ps_suppkey = l_suppkey
    and s_nationkey = n_nationkey
    and n_nationkey = c_nationkey
    and l_orderkey = o_orderkey
    and c_custkey = o_custkey
    and r_regionkey = n_regionkey
