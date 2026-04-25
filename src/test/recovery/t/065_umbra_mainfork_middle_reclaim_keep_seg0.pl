# Verify UMBRA internal reclaim can remove a middle MAIN segment while seg0
# remains present.
#
# Target behavior:
# - relation stays alive
# - middle segment (.1) is physically removed by internal reclaim after checkpoint
# - seg0 file remains present
# - after reclaim, writes on logical seg1 range + checkpoints keep primary/standby consistent
#
# In md mode, skip this test.
use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

$PostgreSQL::Test::Utils::timeout_default = 600;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

sub seg_path
{
	my ($main_path, $segno) = @_;

	return $segno == 0 ? $main_path : "$main_path.$segno";
}

sub highest_existing_segno
{
	my ($node, $main_path, $max_segno) = @_;
	my $highest = 0;

	for my $segno (0 .. $max_segno)
	{
		my $exists = $node->safe_psql(
			'postgres',
			"SELECT COALESCE((pg_stat_file('" . seg_path($main_path, $segno) .
			"', true)).size, -1) >= 0;");
		$highest = $segno if $exists eq 't';
	}

	return $highest;
}

sub ctid_id_band_for_segno
{
	my ($node, $relname, $segno) = @_;
	my $sql = qq{
WITH params AS (
	SELECT (1024::bigint * 1024 * 1024) /
		   current_setting('block_size')::int AS seg_blocks
),
seg AS (
	SELECT min(id) AS min_id,
		   max(id) AS max_id,
		   count(*) AS cnt
	FROM $relname, params
	WHERE split_part(trim(both '()' from ctid::text), ',', 1)::bigint >=
		  $segno * seg_blocks
	  AND split_part(trim(both '()' from ctid::text), ',', 1)::bigint <
		  ($segno + 1) * seg_blocks
)
SELECT COALESCE(min_id::text, '') || '|' ||
	   COALESCE(max_id::text, '') || '|' ||
	   cnt::text
FROM seg;
};
	my ($min_id, $max_id, $cnt) =
	  split(/\|/, $node->safe_psql('postgres', $sql));

	return ($min_id, $max_id, $cnt);
}

my $node = PostgreSQL::Test::Cluster->new('master');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
full_page_writes = on
checkpoint_timeout = '1d'
max_wal_size = '64GB'
wal_keep_size = '1GB'
log_min_messages = debug1
map_superblocks = 50000
map_compactor_enable = off
map_compactor_extent_blocks = 131072
map_compactor_low_live_percent = 60
map_compactor_max_moves = 2000000
mapcompactor_delay = 10ms
mapcompactor_max_relations = 256
mapcompactor_busy_alloc_threshold = 0
});
$node->start();

my $map_mode = $node->safe_psql(
	'postgres', q{
CREATE TABLE umb_reclaim_mid_t(id int PRIMARY KEY, payload text);
ALTER TABLE umb_reclaim_mid_t ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO umb_reclaim_mid_t
SELECT g, repeat('x', 7000) FROM generate_series(1, 300000) g;
SELECT COALESCE(encode(pg_read_binary_file(pg_relation_filepath('umb_reclaim_mid_t') || '_map', 0, 1, true), 'hex'), '') <> '';
});

my $main_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umb_reclaim_mid_t');}
);
my $max_table_id = 300000;
my $phase2_max_update_id = $max_table_id;
my $target_middle_segno = 1;
my ($phase1_min_id, $phase1_max_id, $phase1_cnt) =
  ctid_id_band_for_segno($node, 'umb_reclaim_mid_t', $target_middle_segno);
my $seg_prealloc_bytes = 4 * 1024 * 1024;

is($node->safe_psql(
		'postgres',
		"SELECT COALESCE((pg_stat_file('$main_path', true)).size, -1) >= 0;"),
	't',
	'MAIN seg0 exists after load');
is($node->safe_psql(
			'postgres',
			"SELECT COALESCE((pg_stat_file('$main_path.1', true)).size, -1) >= 0;"),
	't',
	'MAIN seg1 exists after load');
cmp_ok($phase1_cnt, '>', 0, 'identified initial seg1 id band before frontier push');
is($phase1_max_id - $phase1_min_id + 1,
   $phase1_cnt,
   'initial seg1 id band is contiguous');

my $frontier_crossed_seg1 = 'f';
my $seg2_size_after_load = $node->safe_psql(
	'postgres',
	"SELECT COALESCE((pg_stat_file('" . seg_path($main_path, 2) .
	"', true)).size, -1);");
my $update_plan = $node->safe_psql(
	'postgres', qq{
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = on;
EXPLAIN (COSTS OFF)
UPDATE umb_reclaim_mid_t
SET payload = md5((id + 1000000)::text) || repeat('u', 6968)
WHERE id BETWEEN $phase1_min_id AND $phase1_max_id;
});

like($update_plan, qr/Index Scan using umb_reclaim_mid_t_pkey/i,
	'update path uses primary-key index scan');
unlike($update_plan, qr/Seq Scan|Bitmap/i,
	'update path avoids seqscan/bitmapscan');

	$frontier_crossed_seg1 = 't' if $seg2_size_after_load > $seg_prealloc_bytes;

is($frontier_crossed_seg1, 't',
   'initial load already pushed seg2 past the 4MB preallocation watermark');

my ($target_min_id, $target_max_id, $target_cnt) =
  ctid_id_band_for_segno($node, 'umb_reclaim_mid_t', $target_middle_segno);
cmp_ok($target_cnt, '>', 0,
	're-identified current seg1 id band after frontier crossed seg1');
is($target_max_id - $target_min_id + 1,
   $target_cnt,
   'current seg1 id band is contiguous before backup');

$node->backup('bkp_reclaim_mid');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'bkp_reclaim_mid', has_streaming => 1);
$standby->start;
is($standby->safe_psql('postgres',
		q{SELECT pg_relation_filepath('umb_reclaim_mid_t');}),
	$main_path,
	'primary and standby see same relpath');

$node->safe_psql(
	'postgres', q{
ALTER SYSTEM SET map_compactor_enable = on;
SELECT pg_reload_conf();
SELECT pg_sleep(1.0);
});

my $enq_before = $node->safe_psql(
	'postgres',
	q{SELECT pg_stat_get_map_reclaim_enqueued();}
);
my $reclaim_enqueued = 'f';
my ($phase2_min_id, $phase2_max_id, $phase2_cnt);
my ($phase2_update_min_id, $phase2_update_max_id);
my $phase2_floor_id = $phase1_min_id;

# Phase 2: now that the frontier is in seg2+ and standby has copied the shaped
# state, repeatedly cross checkpoint boundaries and rewrite the current seg1 id
# band. Keep the lower bound anchored at the current seg1 band so we do not
# punch seg0, but let the upper bound run past the original 160000-row load to
# keep pressure on tail allocations.
for my $i (1 .. 12)
{
	my $seed = 10000000 + $i * 1000000;

	$node->safe_psql('postgres', q{CHECKPOINT;});
	($phase2_min_id, $phase2_max_id, $phase2_cnt) =
	  ctid_id_band_for_segno($node, 'umb_reclaim_mid_t', $target_middle_segno);
	last if $phase2_cnt <= 0;
	$phase2_update_min_id =
	  $phase2_min_id > $phase2_floor_id ? $phase2_min_id : $phase2_floor_id;
	$phase2_update_max_id = $phase2_max_update_id;
	$node->safe_psql(
		'postgres',
		"SET enable_seqscan = off; " .
		"SET enable_bitmapscan = off; " .
		"SET enable_indexscan = on; " .
		"UPDATE umb_reclaim_mid_t " .
		"SET payload = md5((id + $seed)::text) || repeat('w', 6968) " .
		"WHERE id BETWEEN $phase2_update_min_id AND $phase2_update_max_id;");

	$reclaim_enqueued = $node->safe_psql(
		'postgres',
		"SELECT pg_stat_get_map_reclaim_enqueued() > $enq_before;");
	last if $reclaim_enqueued eq 't';
}

if ($reclaim_enqueued ne 't')
{
	for my $round (1 .. 24)
	{
		$node->safe_psql('postgres', q{CHECKPOINT; SELECT pg_sleep(0.5);});
		$reclaim_enqueued = $node->safe_psql(
			'postgres',
			"SELECT pg_stat_get_map_reclaim_enqueued() > $enq_before;");
		last if $reclaim_enqueued eq 't';
	}
}

is($reclaim_enqueued, 't',
	'compactor enqueued reclaim unlink after seg1 pages were remapped into tail space');

my $middle_seg_removed_on_primary = 'f';
for my $round (1 .. 16)
{
	$node->safe_psql('postgres', q{CHECKPOINT; SELECT pg_sleep(0.2);});

	my $removed = $node->safe_psql(
		'postgres',
		"SELECT COALESCE((pg_stat_file('" .
		seg_path($main_path, $target_middle_segno) .
		"', true)).size, -1) = -1;");
	$middle_seg_removed_on_primary = 't' if $removed eq 't';
	last if $middle_seg_removed_on_primary eq 't';
}
is($middle_seg_removed_on_primary, 't',
	'segment 1 is physically removed by internal reclaim after checkpoint rounds');
ok($node->poll_query_until(
			'postgres',
			"SELECT COALESCE((pg_stat_file('$main_path', true)).size, -1) >= 0;"),
	'MAIN seg0 file still exists after middle-segment reclaim');

my $until_lsn = $node->safe_psql('postgres', q{SELECT pg_current_wal_lsn();});
ok($standby->poll_query_until(
		'postgres',
		"SELECT '$until_lsn'::pg_lsn <= pg_last_wal_replay_lsn();"),
	'standby caught up to reclaim WAL');
ok($standby->poll_query_until(
		'postgres',
		"SELECT COALESCE((pg_stat_file('" .
		seg_path($main_path, $target_middle_segno) .
		"', true)).size, -1) = -1;"),
	'standby segment 1 is physically removed after replay');
ok($standby->poll_query_until(
				'postgres',
				"SELECT COALESCE((pg_stat_file('$main_path', true)).size, -1) >= 0;"),
	'standby MAIN seg0 file still exists after replay');

# Post-reclaim stability round:
# run further writes including logical seg1 ranges, checkpoint, and verify
# primary/standby remain consistent while seg1 stays absent.
$node->safe_psql(
	'postgres', qq{
UPDATE umb_reclaim_mid_t
SET payload = md5((id + 7000000)::text) || repeat('r', 6968)
WHERE id BETWEEN 1 AND 5000;

UPDATE umb_reclaim_mid_t
SET payload = md5((id + 9000000)::text) || repeat('s', 6968)
WHERE id BETWEEN $target_min_id AND $target_max_id;

DELETE FROM umb_reclaim_mid_t WHERE id BETWEEN 20001 AND 21000;
INSERT INTO umb_reclaim_mid_t
SELECT g, md5((g + 12000000)::text) || repeat('n', 6968)
FROM generate_series(20001, 21000) g;

CHECKPOINT;
});

my $stability_lsn = $node->safe_psql('postgres', q{SELECT pg_current_wal_lsn();});
ok($standby->poll_query_until(
		'postgres',
		"SELECT '$stability_lsn'::pg_lsn <= pg_last_wal_replay_lsn();"),
	'standby caught up after post-reclaim write round');

ok($node->poll_query_until(
		'postgres',
		"SELECT COALESCE((pg_stat_file('" .
		seg_path($main_path, $target_middle_segno) .
		"', true)).size, -1) = -1;"),
	'primary segment 1 stays absent after post-reclaim writes');
ok($standby->poll_query_until(
		'postgres',
		"SELECT COALESCE((pg_stat_file('" .
		seg_path($main_path, $target_middle_segno) .
		"', true)).size, -1) = -1;"),
	'standby segment 1 stays absent after replay of post-reclaim writes');

my $primary_fp = $node->safe_psql(
	'postgres', q{
SELECT format('%s|%s|%s|%s',
			  count(*),
			  min(id),
			  max(id),
			  sum((id::bigint * 3 + length(payload))::bigint))
FROM umb_reclaim_mid_t;
});
is($standby->safe_psql(
		'postgres', q{
SELECT format('%s|%s|%s|%s',
			  count(*),
			  min(id),
			  max(id),
			  sum((id::bigint * 3 + length(payload))::bigint))
FROM umb_reclaim_mid_t;
}),
	$primary_fp,
	'primary/standby aggregate fingerprint matches after post-reclaim writes');

my $primary_sample = $node->safe_psql(
	'postgres', qq{
SELECT string_agg(id::text || ':' || left(payload, 12), ',' ORDER BY id)
FROM umb_reclaim_mid_t
WHERE id IN (1, 5000, 20001, 21000, $target_min_id, $target_max_id);
});
is($standby->safe_psql(
		'postgres', qq{
SELECT string_agg(id::text || ':' || left(payload, 12), ',' ORDER BY id)
FROM umb_reclaim_mid_t
WHERE id IN (1, 5000, 20001, 21000, $target_min_id, $target_max_id);
}),
	$primary_sample,
	'primary/standby sample rows match after post-reclaim writes');

is($node->safe_psql('postgres', q{SELECT count(*) FROM umb_reclaim_mid_t;}),
	$max_table_id,
	'relation remains readable after middle-segment reclaim');
is($standby->safe_psql('postgres', q{SELECT count(*) FROM umb_reclaim_mid_t;}),
	$max_table_id,
	'standby relation remains readable after replay');

done_testing();
