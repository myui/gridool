select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			part partitioned by (p_partkey)
			and supplier partitioned by (s_suppkey, s_nationkey)
			and lineitem partitioned by (l_suppkey, l_partkey, l_orderkey)
			and partsupp partitioned by (ps_suppkey)
			and orders partitioned by (o_orderkey)
			and nation partitioned by (n_nationkey)
			and s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%green%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
