select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
	/*
	,part_hidden,
	supplier_hidden,
	partsupp_hidden,
	nation_hidden,
	region_hidden
	*/
from
	<src>
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100;
