select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal
		from
			customer
		where
			customer partitioned by (c_custkey)	-- Invliad filtering?
			and substring(c_phone from 1 for 2) in
				('13', '31', '23', '29', '30', '18', '17')
			and c_acctbal > ( -- requires parallel aggregation
				select
					avg(c_acctbal)
				from
					customer
				where
					customer partitioned by (primarykey)
					and c_acctbal > 0.00
					and substring(c_phone from 1 for 2) in
						('13', '31', '23', '29', '30', '18', '17')
			)
			and not exists (
				select
					*
				from
					orders 
				where
					orders partitioned by (o_custkey)
					and o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;
