select
	cntrycode,
	count(*) as C2,
	sum(c_acctbal) as C3
from
	(
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal
		from
			customer
		where
			customer._hidden & 16 = 16
			and substring(c_phone from 1 for 2) in
				('13', '31', '23', '29', '30', '18', '17')
			and c_acctbal > <src>
			and not exists (
				select
					*
				from
					orders 
				where			
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode

