select
    count(*)
from
    region,
    nation
where
    r_regionkey = n_regionkey
