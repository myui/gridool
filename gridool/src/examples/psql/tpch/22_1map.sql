select
	sum(c_acctbal) as C1,
	count(c_acctbal) as C2
from
	customer
where
	customer._hidden & 16 = 16
	and customer._hidden & 2 = 0
	and c_acctbal > 0.00
	and substring(c_phone from 1 for 2) in
		('13', '31', '23', '29', '30', '18', '17')