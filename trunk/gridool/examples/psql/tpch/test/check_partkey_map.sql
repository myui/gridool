select
    count(*) as C1
from
    part,
    partsupp,
    lineitem
where
	--part partitioned by (p_partkey)
	--and partsupp partitioned by (ps_partkey)
	--and ("partsupp"."_hidden" & 1) = 1
	--and lineitem partitioned by (l_partkey)
    p_partkey = ps_partkey
	and ps_partkey = l_partkey
	and ps_suppkey = l_suppkey