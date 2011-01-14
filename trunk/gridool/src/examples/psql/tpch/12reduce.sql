select
	l_shipmode,
	sum(C2) as high_line_count,
	sum(C3) as low_line_count
from
	<src>
group by
	l_shipmode
order by
	l_shipmode;
