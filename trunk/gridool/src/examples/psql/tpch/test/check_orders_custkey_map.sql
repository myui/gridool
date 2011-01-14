select
	count(o_orderkey) as C1
from
    orders
where
	o_comment not like '%special%requests%'
	and orders._hidden & 16 = 16