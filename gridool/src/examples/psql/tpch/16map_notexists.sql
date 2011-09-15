select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	partsupp partitioned by (ps_partkey, ps_suppkey) -- [TRICK] adding ps_suppkey for partitioning key
	and part partitioned by (p_partkey)
	and p_partkey = ps_partkey
	and p_brand <> 'Brand#45'
	and p_type not like 'MEDIUM POLISHED%'
	and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
	and not exists ( -- false positive may cause on ps_suppkey due to lack of s_suppkey
		select
			null
		from
			supplier
		where
			supplier partitioned by (s_suppkey)
			and s_suppkey = ps_suppkey
			and s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
