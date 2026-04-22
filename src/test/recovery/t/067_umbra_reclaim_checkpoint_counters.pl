# Verify reclaim counters remain sane across checkpoint when punch is disabled.
#
# This is UMBRA-specific and skipped in md mode.
use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('master');
$node->init();
$node->append_conf(
	'postgresql.conf', qq{
autovacuum = off
full_page_writes = on
checkpoint_timeout = '30min'
max_wal_size = '4GB'
log_min_messages = debug1
map_superblocks = 50000
map_compactor_enable = off
map_compactor_extent_blocks = 128
map_compactor_low_live_percent = 100
map_compactor_max_moves = 4096
mapcompactor_delay = 20ms
mapcompactor_max_relations = 128
mapcompactor_busy_alloc_threshold = 0
});
$node->start();

$node->safe_psql('postgres',
	q{CREATE TABLE umb_reclaim_t(id int PRIMARY KEY, payload text);});

$node->safe_psql(
	'postgres', q{
INSERT INTO umb_reclaim_t
SELECT g, repeat('x', 700) FROM generate_series(1, 30000) g;
CHECKPOINT;
UPDATE umb_reclaim_t
SET payload = md5(id::text) || repeat('u', 668)
WHERE id % 2 = 0;
});

$node->safe_psql(
	'postgres', q{
ALTER SYSTEM SET map_compactor_enable = on;
SELECT pg_reload_conf();
SELECT pg_sleep(1.5);
});

ok($node->poll_query_until(
		'postgres',
		q{SELECT pg_stat_get_map_compactor_relocations() > 0;}),
	'map compactor produced relocations');

my ($processed_before, $failed_before) = split(/\|/, $node->safe_psql(
	'postgres',
	q{SELECT pg_stat_get_map_reclaim_processed(),
	          pg_stat_get_map_reclaim_failed();}));
my $attempt_before = $processed_before + $failed_before;

$node->safe_psql('postgres', q{CHECKPOINT;});

ok($node->safe_psql(
		'postgres',
		"SELECT (pg_stat_get_map_reclaim_processed() + pg_stat_get_map_reclaim_failed()) >= $attempt_before;") eq 't',
	'reclaim counters remain monotonic after checkpoint');

my ($processed_after, $failed_after) = split(/\|/, $node->safe_psql(
	'postgres',
	q{SELECT pg_stat_get_map_reclaim_processed(),
	          pg_stat_get_map_reclaim_failed();}));
cmp_ok($processed_after + $failed_after, '>=', $attempt_before,
	'reclaim attempt counters remain monotonic');

is($node->safe_psql('postgres', q{SELECT count(*) FROM umb_reclaim_t;}),
   '30000', 'table remains readable after checkpoint');

done_testing();
