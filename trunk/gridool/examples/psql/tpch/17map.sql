select
	sum(l_extendedprice) as C1
from
	lineitem,
	part
where
	lineitem partitioned by (l_partkey)
	and part partitioned by (p_partkey)
	and p_partkey = l_partkey
	and p_brand = 'Brand#23'
	and p_container = 'MED BOX'
	and l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			lineitem 
		where
			lineitem partitioned by (l_partkey)
			and l_partkey = p_partkey
	);
