# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that page WAL advances a valid but stale mapped root in redo.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

use constant CHUNK_PAGES => 32;
use constant CHUNK_SLOTS => 3;
use constant ROOT_LOGICAL_EOF_OFFSET => 20;

sub expected_capacity
{
	my $logical_eof = shift;

	return 0 if $logical_eof == 0;
	return int(($logical_eof + CHUNK_PAGES - 1) / CHUNK_PAGES)
	  * CHUNK_PAGES * CHUNK_SLOTS;
}

sub relation_blocks
{
	my ($node, $relation) = @_;

	return 0 + $node->safe_psql(
		'postgres',
		"SELECT pg_relation_size('$relation'::regclass) / "
		  . "current_setting('block_size')::integer");
}

sub root_logical_eof
{
	my $path = shift;
	my $root = slurp_file($path);

	return unpack('L', substr($root, ROOT_LOGICAL_EOF_OFFSET, 4));
}

my $node = PostgreSQL::Test::Cluster->new('umbra_logical_birth');
$node->init;
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
checkpoint_timeout = '1h'
max_wal_size = '4GB'
wal_level = replica
]);
$node->start;

$node->safe_psql(
	'postgres', q[
CREATE TABLE umbra_logical_birth (id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false);
INSERT INTO umbra_logical_birth VALUES (1, repeat('b', 700));
CHECKPOINT;
]);

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $main_path = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filepath('umbra_logical_birth'::regclass);]);
my $main_file = $node->data_dir . "/$main_path";
my $root_file = "${main_file}_map";
my $initial_eof = relation_blocks($node, 'umbra_logical_birth');

is(root_logical_eof($root_file), $initial_eof,
	'checkpoint persists the initial logical frontier');

my $wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
$node->safe_psql(
	'postgres', q[
INSERT INTO umbra_logical_birth
SELECT g, repeat('g', 700)
FROM generate_series(2, 700) AS g;
]);
$node->safe_psql(
	'postgres', q[
CREATE INDEX umbra_logical_birth_payload_idx
ON umbra_logical_birth (payload);
CREATE INDEX umbra_logical_birth_payload_gin_idx
ON umbra_logical_birth
USING gin (to_tsvector('simple', payload));
]);
my $wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
my $grown_eof = relation_blocks($node, 'umbra_logical_birth');

cmp_ok($grown_eof, '>', $initial_eof,
	'post-checkpoint insert extends the mapped logical frontier');
is(root_logical_eof($root_file), $initial_eof,
	'uncorrected on-disk root remains at the checkpoint frontier');

my ($dump, $stderr) = run_command(
	[
		'pg_waldump',
		'--path' => $node->data_dir . '/pg_wal',
		'--start' => $wal_start,
		'--end' => $wal_end,
	]);
$stderr =~ s/^pg_waldump: first record is after [0-9A-F]+\/[0-9A-F]+, at [0-9A-F]+\/[0-9A-F]+, skipping over \d+ bytes?\n?//;
is($stderr, '', 'pg_waldump decodes page WAL with Umbra logical EOF metadata');
like($dump, qr/rmgr: .*\b/, 'workload emits ordinary page WAL records');

$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres',
	q[SELECT count(*) FROM umbra_logical_birth WHERE payload = repeat('g', 700)]),
	'699', 'redo reads every page born after the checkpoint');
$node->safe_psql('postgres', 'SET enable_seqscan = off;');
is($node->safe_psql('postgres',
	q[SELECT count(*) FROM umbra_logical_birth WHERE payload = repeat('g', 700)]),
	'699', 'bulk-built index remains readable after redo');
$node->safe_psql('postgres', 'SET enable_indexscan = off;');
is($node->safe_psql('postgres', q[
SELECT count(*) FROM umbra_logical_birth
WHERE to_tsvector('simple', payload) @@
      to_tsquery('simple', repeat('g', 700))]),
	'699', 'range-FPI index remains readable after redo');
is(relation_blocks($node, 'umbra_logical_birth'), $grown_eof,
	'redo restores the logical EOF from page WAL');

$node->safe_psql('postgres', 'CHECKPOINT;');
is(root_logical_eof($root_file), $grown_eof,
	'checkpoint persists the redo-advanced logical frontier');
is(-s $main_file, expected_capacity($grown_eof) * $block_size,
	'redo materializes the physical capacity required by the frontier');

$node->stop;

done_testing();
