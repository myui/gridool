select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp partitioned by (ps_suppkey, ps_partkey)
	supplier partitioned by (s_suppkey, s_nationkey),
	nation partitioned by n_nationkey
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'GERMANY'
group by
	ps_partkey 
having
	sum(ps_supplycost * ps_availqty) >
	(
		select
			sum(ps_supplycost * ps_availqty) * 0.0001000000
		from
			partsupp partitioned by ps_suppkey,
			supplier partitioned by (s_suppkey, s_nationkey),
			nation partitioned by n_nationkey
		where
			ps_suppkey = s_suppkey
			and s_nationkey = n_nationkey
			and n_name = 'GERMANY'
	)
order by
	value desc;
