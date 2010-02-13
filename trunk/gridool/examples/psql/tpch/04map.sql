select
	o_orderpriority,
	count(*) as C2
from
	orders 
where
	orders partitioned by (o_orderkey)
	and o_orderdate >= date '1993-07-01'
	and o_orderdate < date '1993-07-01' + interval '3' month
	and exists (
		select
			*
		from
			lineitem
		where
			lineitem partitioned by (l_orderkey)
			and l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
