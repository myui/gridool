select
	sum(l_extendedprice) as C1
from
	lineitem,
	part
where
	lineitem._hidden & 8 = 8
	-- and lineitem._hidden & 39 <> 0
	and part._hidden & 8 = 8
	-- and part._hidden & 39 <> 0
	and p_partkey = l_partkey
	and p_brand = 'Brand#23'
	and p_container = 'MED BOX'
	and l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			lineitem
		where
			l_partkey = p_partkey
	)
