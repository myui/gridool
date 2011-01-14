select
	sum(ps_supplycost * ps_availqty) as C1
from
	partsupp,
	supplier,
	nation
where
	partsupp._hidden & 32 = 32
	and supplier._hidden & 32 = 32
	and nation._hidden & 96 <> 0
	and ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'GERMANY'