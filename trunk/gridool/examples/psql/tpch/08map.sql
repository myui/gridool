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
			part._hidden & 8 = 8
			and supplier._hidden & 32 = 32
			and lineitem._hidden & 42 <> 0
			and n1._hidden & 16 = 16
			and n2._hidden & 32 = 32
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

