select
    count(*)
from
    region,
    nation
where
	region partitioned by (r_regionkey)
	and nation partitioned by (n_regionkey)
    and r_regionkey = n_regionkey

