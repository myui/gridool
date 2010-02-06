select
	s_name,
	s_address
from
	supplier,
	nation
where
	supplier partitioned by (s_suppkey, s_suppkey)
	and nation partitioned by (n_nationkey)
	and s_suppkey in (
		select
			ps_suppkey
		from
			partsupp
		where
			partsupp partitioned by (ps_partkey, ps_suppkey)
			and ps_partkey in (
				select
					p_partkey
				from
					part
				where
					part partitioned by (p_partkey)
					and p_name like 'forest%'
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					lineitem 
				where
					lineitem partitioned by (l_partkey, l_suppkey)
					and l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= date '1994-01-01'
					and l_shipdate < date '1994-01-01' + interval '1' year
			)
	)
	and s_nationkey = n_nationkey
	and n_name = 'CANADA'
order by
	s_name;
