select
    count(*)
from
    nation,
    supplier,
    customer
where
	n_nationkey = s_nationkey
    and n_nationkey = c_nationkey
