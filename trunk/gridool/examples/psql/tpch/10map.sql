select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as C3,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	lineitem._hidden & 2 = 2
	and orders._hidden & 2 = 2
	and customer._hidden & 18 <> 0
	and nation._hidden & 80 <> 0
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '1993-10-01'
	and o_orderdate < date '1993-10-01' + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
