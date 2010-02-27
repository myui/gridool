
		select
			c_custkey,
			sum(C2)
		from
			<src>
		group by
			c_custkey
		order by c_custkey