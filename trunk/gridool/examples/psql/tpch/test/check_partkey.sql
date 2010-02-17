select
    count(*)
from
    part,
    partsupp,
    lineitem
where
    p_partkey = ps_partkey 
    and ps_partkey = l_partkey
