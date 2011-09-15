select
    count(*) as C1
from
    lineitem,
    partsupp
where
	lineitem partitioned by (l_partkey,l_suppkey)
    and partsupp partitioned by (ps_partkey,ps_suppkey)
	-- ("lineitem"."_hidden" & 2) = 2
	-- and ("partsupp"."_hidden" & 1) = 1
    and l_partkey = ps_partkey 
    and l_suppkey = ps_suppkey
