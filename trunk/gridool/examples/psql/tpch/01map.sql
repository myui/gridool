select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as C3,
	sum(l_extendedprice) as C4,
	sum(l_extendedprice * (1 - l_discount)) as C5,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as C6,
	sum(l_quantity) as C7,
	sum(l_extendedprice) as C8,
	sum(l_discount) as C9,
	count(*) as C10
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '90' day (3)
group by
	l_returnflag,
	l_linestatus

