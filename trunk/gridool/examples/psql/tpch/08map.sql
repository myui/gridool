select
	o_year,
	sum(case
		when nation = 'BRAZIL' then volume
		else 0
	end) as AGR1, 
	sum(volume) as AGR2
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from
			part,
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2,
			region
		where
			part partitioned by (p_partkey)
			and supplier partitioned by (s_suppkey, s_nationkey)
			and lineitem partitioned by (l_partkey, l_suppkey, l_orderkey)
			and orders partitioned by (o_orderkey, o_custkey)
			and customer partitioned by (c_custkey, c_nationkey)
			and nation partitioned by (n_nationkey, n_regionkey) alias n1
			and nation partitioned by (n_nationkey) alias n2
			and region partitioned by (r_regionkey)
			and p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = 'AMERICA'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = 'ECONOMY ANODIZED STEEL'
	) as all_nations
group by
	o_year

