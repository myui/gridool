select
	s_name,
	count(*) as numwait
from
	supplier,
	lineitem l1,
	orders,
	nation
where
	supplier partitioned by (s_suppkey, s_nationkey)
	and lineitem partitioned by (l_suppkey, l_orderkey) alias l1
	and orders partitioned by (o_orderkey)
	and nation partitioned by (n_nationkey)
	and s_suppkey = l1.l_suppkey
	and o_orderkey = l1.l_orderkey
	and o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and exists (
		select
			*
		from
			lineitem l2 
		where
			lineitem partitioned by (l_orderkey) alias l2
			and l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey
	)
	and not exists ( -- Outer table is already partitioned by l_orderkey (Self-Join)
		select
			*
		from
			lineitem l3 
		where
			lineitem partitioned by (l_orderkey) alias l3
			and l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.l_receiptdate > l3.l_commitdate
	)
	and s_nationkey = n_nationkey
	and n_name = 'SAUDI ARABIA'
group by
	s_name
order by
	numwait desc,
	s_name
limit 100;
