select
	s_name,
	s_address
from
	supplier,
	nation
where
	supplier._hidden & 64 = 64
	and nation._hidden & 64 = 64
	and s_suppkey in (
		select
			ps_suppkey
		from
			partsupp
		where
--			partsupp._hidden & 8 = 8 and
			ps_partkey in (
				select
					p_partkey
				from
					part
				where
--					part._hidden & 8 = 8 and
					p_name like 'forest%'
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					lineitem 
				where
--					lineitem._hidden = 8 and
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= date '1994-01-01'
					and l_shipdate < date '1994-01-01' + interval '1' year
			)
	)
	and s_nationkey = n_nationkey
	and n_name = 'CANADA'
