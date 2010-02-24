select 
    count(1) as C1
from 
    lineitem 
         inner join 
    orders 
        on l_orderkey = o_orderkey
    inner join
    customer
        on o_custkey = c_custkey
