select 
    count(1) as C1
from
    part,
    partsupp,
    lineitem
where
    p_partkey = ps_partkey
    and ps_partkey = l_partkey
    and ps_suppkey = l_suppkey