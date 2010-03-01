select
	cntrycode,
	sum(C2) as numcust,
	sum(C3) as totacctbal
from
	<src>
group by
	cntrycode
order by
	cntrycode;
