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
	part,
	supplier,
	partsupp,
	nation,
	region
where
	part partitioned by (p_partkey) 
	and supplier partitioned by (s_suppkey, s_nationkey) 
	and partsupp partitioned by (ps_partkey, ps_suppkey) 
	and nation partitioned by (n_nationkey, n_regionkey) 
 	and region partitioned by (r_regionkey) 
	and p_partkey = ps_partkey
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
			partsupp,
			supplier,
			nation,
			region
		where
			partsupp partitioned by (ps_partkey, ps_suppkey)
			and supplier partitioned by (s_suppkey, s_nationkey)
			and nation partitioned by (n_nationkey, n_regionkey)
			and region partitioned by (r_regionkey)
			and p_partkey = ps_partkey
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
