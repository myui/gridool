select
    count(*) as C1
from
    lineitem,
    partsupp
where
	lineitem partitioned by (l_partkey,l_suppkey)
    and partsupp partitioned by (ps_partkey,ps_suppkey)
    and l_partkey = ps_partkey 
    and l_suppkey = ps_suppkey
