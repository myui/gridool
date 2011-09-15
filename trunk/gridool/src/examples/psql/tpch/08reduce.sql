select
	o_year,
	sum(AGR1) / sum(AGR2) as mkt_share
from
	<src>
group by
	o_year
order by
	o_year;
