select
    count(*) as C1
from
    customer
where
	customer._hidden & 16 = 16
