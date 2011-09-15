select
    count(*)
from
    partsupp,
    supplier,
    lineitem
where
    ps_suppkey = s_suppkey
	and ps_suppkey = l_suppkey

