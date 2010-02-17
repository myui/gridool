select
    count(*)
from
    nation,
    supplier,
    costomer
where
    ps_suppkey = s_suppkey
    and n_nationkey = c_nationkey
