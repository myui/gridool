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
			partsupp partitioned by (ps_suppkey)
			and supplier partitioned by (s_suppkey, s_nationkey)
			and nation partitioned by (n_nationkey)
			and ps_suppkey = s_suppkey
			and s_nationkey = n_nationkey
			and n_name = 'GERMANY'
	)
order by
	value desc;
