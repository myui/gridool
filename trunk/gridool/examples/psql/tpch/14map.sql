select
	sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) as AGR1,
	sum(l_extendedprice * (1 - l_discount)) as AGR2
from
	lineitem,
	part
where
	lineitem partitioned by (l_partkey)
	and part partitioned by (p_partkey)
	and l_partkey = p_partkey
	and l_shipdate >= date '1995-09-01'
	and l_shipdate < date '1995-09-01' + interval '1' month;
