select
	ps_partkey,
	sum(C2) as value
from
	<src>
group by
	ps_partkey 
having
	sum(ps_supplycost * ps_availqty) >
	(
		select
			sum(ps_supplycost * ps_availqty) * 0.0001000000
		from
			partsupp,
			supplier,
			nation
		where
			(partsupp._hidden & 4) = 4
			and ps_suppkey = s_suppkey
			and s_nationkey = n_nationkey
			and n_name = 'GERMANY'
	)
order by
	value desc;
