# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify the chunk-paired Umbra layout on the first correctness path:
# boundary logical blocks remain readable through checkpoint, restart,
# sequential scan, rewrite, truncate, and drop.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

my ($chunk_pages) =
  scan_server_header('storage/umbra.h',
	q{#define UMBRA_CHUNK_PAIRED_PAGES ([0-9]+)U});
my ($active_slots) =
  scan_server_header('storage/umbra.h',
	q{#define UMBRA_CHUNK_ACTIVE_SLOTS ([0-9]+)U});
my @boundary_blocks = (
	0,
	$chunk_pages - 1,
	$chunk_pages,
	(2 * $chunk_pages) - 1,
	2 * $chunk_pages);

sub chunk_capacity_for_lblk
{
	my ($lblk) = @_;

	return (int($lblk / $chunk_pages) + 1) *
	  ($active_slots * $chunk_pages);
}

sub u32le_from_hex
{
	my ($hex, $offset) = @_;
	my $chunk = substr($hex, $offset * 2, 8);
	my @b = ($chunk =~ /../g);

	return hex($b[0]) +
	  (hex($b[1]) << 8) +
	  (hex($b[2]) << 16) +
	  (hex($b[3]) << 24);
}

sub relation_signature_sql
{
	return q[
SELECT count(*) || '|' ||
       sum(id)::bigint || '|' ||
       md5(string_agg(id::text || ':' || payload, ',' ORDER BY id))
FROM umb_chunk_pair_t;
];
}

sub verify_chunk_metadata
{
	my ($node, $relname, $max_lblk) = @_;

	my $super_hex = $node->safe_psql(
		'postgres',
		"SELECT encode(pg_read_binary_file(pg_relation_filepath('$relname') || '_map', 0, 64, true), 'hex');"
	);
	my $phys_capacity_main = u32le_from_hex($super_hex, 20);
	my $logical_main = u32le_from_hex($super_hex, 40);

	cmp_ok($logical_main, '>', $max_lblk,
		'metadata tracks logical EOF past checked chunk boundary');
	cmp_ok($phys_capacity_main, '>=', chunk_capacity_for_lblk($max_lblk),
		'metadata publishes chunk-paired physical capacity');
}

my $node = PostgreSQL::Test::Cluster->new('umbra_chunk_paired_smoke');
$node->init();
$node->append_conf(
	'postgresql.conf', qq[
wal_level = 'replica'
autovacuum = off
full_page_writes = on
shared_buffers = '256MB'
max_wal_size = '4GB'
min_wal_size = '1GB'
checkpoint_timeout = '1h'
]);
$node->start();

$node->safe_psql('postgres', q[
CREATE TABLE umb_chunk_pair_t(id int, payload text);
ALTER TABLE umb_chunk_pair_t ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO umb_chunk_pair_t
SELECT g, repeat('x', 7000)
FROM generate_series(1, 180) AS g;
CHECKPOINT;
]);

my $present_blocks = $node->safe_psql(
	'postgres', q[
WITH heap_blocks AS (
	SELECT DISTINCT split_part(trim(both '()' from ctid::text), ',', 1)::int AS blk
	FROM umb_chunk_pair_t
)
SELECT string_agg(blk::text, ',' ORDER BY blk)
FROM heap_blocks
WHERE blk IN (0, 31, 32, 63, 64);
]);
is($present_blocks, '0,31,32,63,64',
	'fixture covers chunk boundary logical blocks');

$node->safe_psql('postgres', q[
UPDATE umb_chunk_pair_t
SET payload = md5(id::text) || repeat('u', 6968)
WHERE split_part(trim(both '()' from ctid::text), ',', 1)::int
      IN (0, 31, 32, 63, 64);
]);

my $signature_after_update =
  $node->safe_psql('postgres', relation_signature_sql());

$node->safe_psql('postgres', q[CHECKPOINT;]);
verify_chunk_metadata($node, 'umb_chunk_pair_t', 64);
is($node->safe_psql('postgres', relation_signature_sql()),
	$signature_after_update,
	'sequential scan sees updated rows before restart');

$node->restart();
is($node->safe_psql('postgres', relation_signature_sql()),
	$signature_after_update,
	'sequential scan sees updated rows after restart');

$node->safe_psql('postgres', q[VACUUM FULL umb_chunk_pair_t;]);
is($node->safe_psql('postgres', relation_signature_sql()),
	$signature_after_update,
	'relation rewrite preserves chunk-paired contents');

my $drop_map_path = $node->safe_psql(
	'postgres', q[
CREATE TABLE umb_chunk_pair_drop_t(id int, payload text);
INSERT INTO umb_chunk_pair_drop_t
SELECT g, repeat('d', 100) FROM generate_series(1, 1000) AS g;
SELECT pg_relation_filepath('umb_chunk_pair_drop_t') || '_map';
]);

$node->safe_psql('postgres', q[
TRUNCATE umb_chunk_pair_t;
INSERT INTO umb_chunk_pair_t
SELECT g, repeat('z', 7000)
FROM generate_series(1, 40) AS g;
DROP TABLE umb_chunk_pair_drop_t;
CHECKPOINT;
]);

is($node->safe_psql('postgres',
		q[SELECT count(*) || '|' || sum(id)::bigint FROM umb_chunk_pair_t;]),
	'40|820',
	'truncate and reload remain readable');

ok($node->poll_query_until('postgres',
	"SELECT COALESCE(encode(pg_read_binary_file('$drop_map_path', 0, 1, true), 'hex'), '') = '';",
	't'),
	'dropped relation MAP fork disappears after checkpoint');

$node->stop();

done_testing();
