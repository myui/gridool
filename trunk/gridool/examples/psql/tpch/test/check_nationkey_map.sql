select
    count(*)
from
    nation,
    supplier,
    customer
where
    nation partitioned by (n_nationkey)
	and supplier partitioned by (s_nationkey)
	and customer partitioned by (c_nationkey)
    and n_nationkey = s_nationkey
    and n_nationkey = c_nationkey
