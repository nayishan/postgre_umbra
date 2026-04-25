# Verify map compactor relocation survives crash restart.
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
log_min_messages = debug1
map_superblocks = 50000
map_compactor_enable = off
map_compactor_extent_blocks = 128
map_compactor_low_live_percent = 100
map_compactor_max_moves = 64
mapcompactor_delay = 10ms
mapcompactor_max_relations = 64
mapcompactor_busy_alloc_threshold = 0
});
$node->start();

$node->safe_psql('postgres',
	q{CREATE TABLE umb_compact_t(id int PRIMARY KEY, payload text);});

$node->safe_psql(
	'postgres', q{
INSERT INTO umb_compact_t
SELECT g, repeat('x', 700) FROM generate_series(1, 25000) g;
CHECKPOINT;
UPDATE umb_compact_t
SET payload = md5(id::text) || repeat('u', 668)
WHERE id % 2 = 0;
});

$node->safe_psql(
	'postgres', q{
ALTER SYSTEM SET map_compactor_enable = on;
SELECT pg_reload_conf();
SELECT pg_sleep(1.0);
});

ok($node->poll_query_until(
		'postgres',
		q{SELECT pg_stat_get_map_compactor_relocations() > 0;}),
	'map compactor relocation stats become visible');

my $before = $node->safe_psql(
	'postgres', q{
SELECT count(*) || ',' ||
	   sum(length(payload))::bigint || ',' ||
	   sum(id)::bigint
FROM umb_compact_t;
});

$node->stop('immediate');
$node->start();

my $after = $node->safe_psql(
	'postgres', q{
SELECT count(*) || ',' ||
	   sum(length(payload))::bigint || ',' ||
	   sum(id)::bigint
FROM umb_compact_t;
});

is($after, $before, 'aggregate state preserved after compactor + crash restart');

my $idx_count = $node->safe_psql(
	'postgres', q{
SET enable_seqscan = off;
SELECT count(*) FROM umb_compact_t WHERE id BETWEEN 100 AND 24000;
});
my $seq_count = $node->safe_psql(
	'postgres', q{
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM umb_compact_t WHERE id BETWEEN 100 AND 24000;
});
is($idx_count, $seq_count, 'index path and seq path return same rowcount');

done_testing();
