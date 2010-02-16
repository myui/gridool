select
    count(*)
from
    part,
    partsupp
where
    p_partkey = ps_partkey
