select
    count(*) as C1
from
    partsupp,
    supplier,
    lineitem
where
	partsupp partitioned by (ps_suppkey)
	and supplier partitioned by (s_suppkey)
    and ps_suppkey = s_suppkey
    and ps_suppkey = l_suppkey