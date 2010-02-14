select
	s_name,
	sum(C2) as numwait
from
	<src>
group by
	s_name
order by
	numwait desc,
	s_name
limit 100;
