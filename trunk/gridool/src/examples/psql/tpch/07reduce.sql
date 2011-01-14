select
	supp_nation,
	cust_nation,
	l_year,
	sum(C4) as revenue
from
	<src>
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;
