select
    count(*)
from
    customer,
    orders
where
	customer partitioned by (c_custkey)
	and orders partitioned by (o_custkey)
    and c_custkey = o_custkey
