select
	p_brand,
	p_type,
	p_size,
	sum(C4) as supplier_cnt
from
	<src>
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;
