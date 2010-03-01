select
    count(*) as C1
from
    customer,
    orders
where
	customer._hidden & 16 = 16
	and orders._hidden & 16 = 16
    and c_custkey = o_custkey
