select
	sum(l_extendedprice * l_discount) as C2
from
	lineitem
where
	lineitem._hidden & 1 = 1
	and l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1' year
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24
