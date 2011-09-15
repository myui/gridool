select * from (
	select
		c_custkey,
		count(o_orderkey) as C2
	from
		customer 
			inner join 
		orders 
			on c_custkey = o_custkey
			and o_comment not like '%special%requests%'
	where 
		orders._hidden & 16 = 16
		and customer._hidden & 16 = 16
	group by
		c_custkey
	union all 
	select 
		c_custkey,
		0 as C2
	from
		customer as c
	where
		c._hidden & 16 = 16
		and c.c_custkey not in 
			(select o_custkey from orders)
) as c_orders (c_custkey, C2)