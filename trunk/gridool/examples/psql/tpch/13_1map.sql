select
	c_custkey,
	count(o_orderkey) as C2
from
	customer 
		left outer join 
	orders 
		on c_custkey = o_custkey
		and o_comment not like '%special%requests%'
where 
	(orders._hidden & 16 = 16) and
	(customer._hidden & 16 = 16)
group by
	c_custkey
