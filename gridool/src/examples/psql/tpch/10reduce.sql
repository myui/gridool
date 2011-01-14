select
	c_custkey,
	c_name,
	sum(C3) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	<src>
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
limit 20;
