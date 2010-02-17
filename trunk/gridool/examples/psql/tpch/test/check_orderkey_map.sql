select
    count(*)
from
    orders,
    lineitem
where
	orders partitioned by (o_orderkey)
	and lineitem partitioned by (l_orderkey)
    and o_orderkey = l_orderkey
