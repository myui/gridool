select
    count(*) as C1
from
    part,
    partsupp,
    lineitem
where
	part partitioned by (p_partkey)
	and partsupp partitioned by (ps_partkey)
    and p_partkey = ps_partkey
	and ps_partkey = l_partkey