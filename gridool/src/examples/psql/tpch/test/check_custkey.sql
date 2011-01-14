select
    count(*)
from
    customer,
    orders
where
    c_custkey = o_custkey


