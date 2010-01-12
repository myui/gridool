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
	part partitioned by p_partkey,
	supplier partitioned by s_suppkey and s_nationkey,
	partsupp partitioned by ps_partkey and ps_suppkey,
	nation partitioned by n_nationkey and n_regionkey,
	region partitioned by r_regionkey
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 15
	and p_type like '%BRASS'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'EUROPE'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			partsupp partitioned by ps_partkey and ps_suppkey,
			supplier partitioned by s_suppkey and s_nationkey,
			nation partitioned by n_nationkey and n_regionkey,
			region partitioned by r_regionkey
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'EUROPE'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100;
