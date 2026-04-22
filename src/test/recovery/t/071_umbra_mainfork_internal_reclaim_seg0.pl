# Verify UMBRA internal reclaim can physically remove MAIN seg0.
#
# This test targets internal physical deletion (reclaim), not DROP/TRUNCATE:
# - keep the relation alive
# - churn only a tail key range until MAIN seg1 exists
# - keep a large untouched prefix so seg0 must still have live mappings
# - enable compactor and use checkpoint rounds to flush map state to disk
# - verify seg0 disappears while seg1 still exists
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

my $node = PostgreSQL::Test::Cluster->new('master');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
full_page_writes = on
checkpoint_timeout = '30min'
max_wal_size = '4GB'
log_min_messages = debug1
map_superblocks = 50000
map_compactor_enable = off
map_compactor_extent_blocks = 131072
map_compactor_low_live_percent = 100
map_compactor_max_moves = 2000000
mapcompactor_delay = 10ms
mapcompactor_max_relations = 50000
mapcompactor_busy_alloc_threshold = 0
});
$node->start();

my $map_mode = $node->safe_psql(
	'postgres', q{
CREATE OR REPLACE FUNCTION umb_count_mapped_in_seg(rel regclass, segno int)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
	path text;
	bs int;
	seg_blocks bigint;
	nblocks bigint;
	lblk bigint;
	fork_page_idx bigint;
	group_no bigint;
	page_no bigint;
	cur_page_no bigint := -1;
	page bytea;
	off int;
	pblk bigint;
	seg_lo bigint;
	seg_hi bigint;
	cnt bigint := 0;
BEGIN
	path := pg_relation_filepath(rel) || '_map';
	bs := current_setting('block_size')::int;
	seg_blocks := (1024::bigint * 1024 * 1024) / bs;
	seg_lo := segno::bigint * seg_blocks;
	seg_hi := seg_lo + seg_blocks;
	nblocks := pg_relation_size(rel) / bs;

	FOR lblk IN 0 .. (nblocks - 1) LOOP
		fork_page_idx := lblk / 2048;
		group_no := fork_page_idx / 8192;
		page_no := 3 + group_no * 8194 + (fork_page_idx % 8192);
		IF page_no <> cur_page_no THEN
			page := pg_read_binary_file(path, page_no * bs, bs, true);
			cur_page_no := page_no;
		END IF;

		off := ((lblk % 2048) * 4)::int;
		IF page IS NULL OR length(page) < off + 4 THEN
			CONTINUE;
		END IF;
		pblk := get_byte(page, off)::bigint
			  + (get_byte(page, off + 1)::bigint << 8)
			  + (get_byte(page, off + 2)::bigint << 16)
			  + (get_byte(page, off + 3)::bigint << 24);

		IF pblk <> 4294967295::bigint AND pblk >= seg_lo AND pblk < seg_hi THEN
			cnt := cnt + 1;
		END IF;
	END LOOP;

	RETURN cnt;
END;
$$;

CREATE TABLE umb_reclaim_seg0_t(id int PRIMARY KEY, payload text);
INSERT INTO umb_reclaim_seg0_t
SELECT g, repeat('x', 2000) FROM generate_series(1, 40000) g;
SELECT COALESCE(encode(pg_read_binary_file(pg_relation_filepath('umb_reclaim_seg0_t') || '_map', 0, 1, true), 'hex'), '') <> '';
});

my $main_path = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_filepath('umb_reclaim_seg0_t');}
);

$node->backup('bkp_reclaim_seg0');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'bkp_reclaim_seg0', has_streaming => 1);
$standby->start;
is($standby->safe_psql('postgres',
		q{SELECT pg_relation_filepath('umb_reclaim_seg0_t');}),
	$main_path,
	'primary and standby see same relpath');

my $logical_blocks = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_size('umb_reclaim_seg0_t') / current_setting('block_size')::int;}
);
cmp_ok($logical_blocks, '>', 0, 'table has non-zero logical blocks');
cmp_ok($logical_blocks, '<', 131072,
	'table initially fits in MAIN seg0 before churn');

my $seg1_size = -1;
is($seg1_size, -1,
	'MAIN seg1 does not exist before boundary-crossing partial churn');

my $mapped_seg0 = $node->safe_psql(
	'postgres',
	q{SELECT umb_count_mapped_in_seg('umb_reclaim_seg0_t'::regclass, 0);}
);
cmp_ok($mapped_seg0, '>', 0,
	'current mappings initially reside in seg0 before final crossing phase');

# Drive the physical frontier to seg1 by repeatedly updating only the tail
# portion of the table. The untouched prefix should keep seg0 live, while the
# churned tail should eventually push new mappings into seg1.
my $cross_rounds = 0;
while ($seg1_size < 16 * 1024 * 1024)
{
	$cross_rounds++;
	die "tail churn did not push MAIN seg1 past prealloc-only size\n"
		if $cross_rounds > 64;

	$node->safe_psql(
		'postgres',
		"UPDATE umb_reclaim_seg0_t " .
		"SET payload = md5((id + ($cross_rounds * 1000000))::text) || repeat('u', 1968) " .
		"WHERE id > 20000;");

	$seg1_size = $node->safe_psql(
		'postgres',
		"SELECT COALESCE((pg_stat_file('$main_path.1', true)).size, -1);");
}

cmp_ok($seg1_size, '>=', 16 * 1024 * 1024,
	'MAIN seg1 grew beyond prealloc-only tail after controlled churn');

# The physical frontier can cross into seg1 before any *current* mapping is
# redirected there. Keep churning the tail until the relation's logical size
# itself also extends beyond seg0, guaranteeing that some live mappings now
# reside in seg1.
my $post_cross_logical = $node->safe_psql(
	'postgres',
	q{SELECT pg_relation_size('umb_reclaim_seg0_t') / current_setting('block_size')::int;}
);
my $post_cross_round = 0;
while ($post_cross_logical <= 140000)
{
	$post_cross_round++;
	die "post-cross tail churn did not push logical blocks into seg1\n"
		if $post_cross_round > 24;

	$node->safe_psql(
		'postgres',
		"UPDATE umb_reclaim_seg0_t " .
		"SET payload = md5((id + (9000000 + $post_cross_round * 1000000))::text) || repeat('v', 1968) " .
		"WHERE id > 20000;");
	$post_cross_logical = $node->safe_psql(
		'postgres',
		q{SELECT pg_relation_size('umb_reclaim_seg0_t') / current_setting('block_size')::int;}
	);
}
cmp_ok($post_cross_logical, '>', 131072,
	'logical relation size extends into seg1 before compactor reclaim');

is($node->safe_psql(
		'postgres',
		"SELECT COALESCE((pg_stat_file('$main_path', true)).size, -1) >= 0;"),
	't',
	'MAIN seg0 still exists before compactor reclaim');

is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM umb_reclaim_seg0_t WHERE id <= 20000;}),
	'20000',
	'untouched prefix remains visible before compactor reclaim');

my $reloc_before = $node->safe_psql(
	'postgres',
	q{SELECT pg_stat_get_map_compactor_relocations();}
);
my $reclaim_processed_before = $node->safe_psql(
	'postgres',
	q{SELECT pg_stat_get_map_reclaim_processed();}
);
my $reclaim_enqueued_before = $node->safe_psql(
	'postgres',
	q{SELECT pg_stat_get_map_reclaim_enqueued();}
);

$node->safe_psql(
	'postgres', q{
ALTER SYSTEM SET map_compactor_enable = on;
SELECT pg_reload_conf();
});

my $removed_on_primary = 'f';
my $reclaim_processed = 'f';
my $reloc_advanced = 'f';
my $reclaim_enqueued = 'f';
for my $round (1 .. 60)
{
	$node->safe_psql('postgres', q{CHECKPOINT; SELECT pg_sleep(0.5);});
	$reloc_advanced = $node->safe_psql(
		'postgres',
		"SELECT pg_stat_get_map_compactor_relocations() > $reloc_before;");
	last if $reloc_advanced eq 't';
}
is($reloc_advanced, 't',
	'compactor relocation counter advanced before internal reclaim completed');
for my $round (1 .. 120)
{
	$node->safe_psql('postgres', q{CHECKPOINT; SELECT pg_sleep(0.5);});
	$reclaim_enqueued = $node->safe_psql(
		'postgres',
		"SELECT pg_stat_get_map_reclaim_enqueued() > $reclaim_enqueued_before;");
	last if $reclaim_enqueued eq 't';
}

for my $round (1 .. 20)
{
	$node->safe_psql('postgres', q{CHECKPOINT; SELECT pg_sleep(1.0);});
	$reclaim_processed = $node->safe_psql(
		'postgres',
		"SELECT pg_stat_get_map_reclaim_processed() > $reclaim_processed_before;");
	$removed_on_primary = $node->safe_psql(
		'postgres',
		"SELECT COALESCE((pg_stat_file('$main_path', true)).size, -1) = -1;");
	last if $reclaim_processed eq 't' && $removed_on_primary eq 't';
}
is($reclaim_processed, 't',
	'internal reclaim processed seg0 after compactor relocation');
is($removed_on_primary, 't',
	'MAIN seg0 is physically removed by internal reclaim after checkpoint rounds');
$reclaim_enqueued = $node->safe_psql(
	'postgres',
	"SELECT pg_stat_get_map_reclaim_enqueued() > $reclaim_enqueued_before;");
is($reclaim_enqueued, 't',
	'internal reclaim was enqueued before seg0 was physically removed');

my $until_lsn = $node->safe_psql('postgres', q{SELECT pg_current_wal_lsn();});
ok($standby->poll_query_until(
		'postgres',
		"SELECT '$until_lsn'::pg_lsn <= pg_last_wal_replay_lsn();"),
	'standby caught up to reclaim WAL');
ok($standby->poll_query_until(
		'postgres',
		"SELECT COALESCE((pg_stat_file('$main_path', true)).size, -1) = -1;"),
	'standby MAIN seg0 is physically removed after replay');

is($node->safe_psql('postgres', q{SELECT count(*) FROM umb_reclaim_seg0_t;}),
	'40000',
	'relation remains readable after internal seg0 reclaim');
is($standby->safe_psql('postgres', q{SELECT count(*) FROM umb_reclaim_seg0_t;}),
	'40000',
	'standby relation remains readable after replay');

done_testing();
