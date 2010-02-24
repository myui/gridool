select 
    count(1)
from
    lineitem,
    orders,
    customer
where
    l_orderkey = o_orderkey
    and o_custkey = c_custkey