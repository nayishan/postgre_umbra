# Verify remap-heavy workload remains consistent after crash restart.
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
});
$node->start();

$node->safe_psql('postgres',
	q{CREATE TABLE umb_remap_t(id int PRIMARY KEY, payload text);});

$node->safe_psql(
	'postgres', q{
CREATE INDEX umb_remap_payload_idx ON umb_remap_t ((left(payload, 16)));
INSERT INTO umb_remap_t
SELECT g, repeat('a', 320) FROM generate_series(1, 30000) g;
CHECKPOINT;
UPDATE umb_remap_t
SET payload = md5(id::text) || repeat('u', 280)
WHERE id % 3 = 0;
DELETE FROM umb_remap_t WHERE id % 17 = 0;
INSERT INTO umb_remap_t
SELECT g, repeat('n', 320) FROM generate_series(30001, 32000) g;
});

my $before = $node->safe_psql(
	'postgres', q{
SELECT count(*) || ',' ||
	   sum(length(payload))::bigint || ',' ||
	   sum(id)::bigint
FROM umb_remap_t;
});

$node->stop('immediate');
$node->start();

my $after = $node->safe_psql(
	'postgres', q{
SELECT count(*) || ',' ||
	   sum(length(payload))::bigint || ',' ||
	   sum(id)::bigint
FROM umb_remap_t;
});

is($after, $before, 'aggregate state preserved across crash restart');

my $idx_count = $node->safe_psql(
	'postgres', q{
SET enable_seqscan = off;
SELECT count(*) FROM umb_remap_t WHERE id BETWEEN 100 AND 30000;
});
my $seq_count = $node->safe_psql(
	'postgres', q{
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM umb_remap_t WHERE id BETWEEN 100 AND 30000;
});
is($idx_count, $seq_count, 'index path and seq path return same rowcount');

done_testing();
