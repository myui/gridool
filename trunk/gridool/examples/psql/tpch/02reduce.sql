select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	<src>
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100;
