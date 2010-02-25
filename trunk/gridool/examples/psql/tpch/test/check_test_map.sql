select 
    count(1) as C1
from 
    lineitem,
    orders,
    customer
where	
	(lineitem._hidden & 1 = 1)
	and l_orderkey = o_orderkey
	and o_custkey = c_custkey
