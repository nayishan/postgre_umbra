# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify redo derives logical EOF from exact three-bucket physical length.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires an Umbra storage manager build'
  unless check_pg_config('^#define USE_UMBRA 1$');

sub relation_blocks
{
	my ($node, $relation) = @_;

	return 0 + $node->safe_psql(
		'postgres',
		"SELECT pg_relation_size('$relation'::regclass) / "
		  . "current_setting('block_size')::integer");
}

sub check_physical_eof
{
	my ($node, $relation, $main_file, $block_size, $label) = @_;
	my $logical_eof = relation_blocks($node, $relation);

	is(-s $main_file, 3 * $logical_eof * $block_size,
		"$label: physical length is exactly three times logical EOF");
	return $logical_eof;
}

my $node = PostgreSQL::Test::Cluster->new('umbra_physical_eof_redo');
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
CHECKPOINT;
CREATE TABLE umbra_physical_eof (id integer PRIMARY KEY, payload text)
  WITH (autovacuum_enabled = false);
INSERT INTO umbra_physical_eof VALUES (1, repeat('b', 700));
CHECKPOINT;
]);

my $block_size = 0 + $node->safe_psql('postgres', 'SHOW block_size');
my $main_path = $node->safe_psql(
	'postgres',
	q[SELECT pg_relation_filepath('umbra_physical_eof'::regclass);]);
my $main_file = $node->data_dir . "/$main_path";
my $initial_eof = check_physical_eof($node, 'umbra_physical_eof',
	$main_file, $block_size, 'initial checkpoint');

my $wal_start = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
$node->safe_psql(
	'postgres', q[
INSERT INTO umbra_physical_eof
SELECT g, repeat('g', 700)
FROM generate_series(2, 700) AS g;
CREATE INDEX umbra_physical_eof_payload_idx
ON umbra_physical_eof (payload);
CREATE INDEX umbra_physical_eof_payload_gin_idx
ON umbra_physical_eof
USING gin (to_tsvector('simple', payload));
]);
my $wal_end = $node->safe_psql(
	'postgres', 'SELECT pg_current_wal_insert_lsn();');
my $grown_eof = check_physical_eof($node, 'umbra_physical_eof',
	$main_file, $block_size, 'post-checkpoint growth');

cmp_ok($grown_eof, '>', $initial_eof,
	'post-checkpoint inserts extend the physical EOF');

my ($dump, $stderr) = run_command(
	[
		'pg_waldump',
		'--path' => $node->data_dir . '/pg_wal',
		'--start' => $wal_start,
		'--end' => $wal_end,
	]);
$stderr =~ s/^pg_waldump: first record is after [0-9A-F]+\/[0-9A-F]+, at [0-9A-F]+\/[0-9A-F]+, skipping over \d+ bytes?\n?//;
is($stderr, '', 'pg_waldump decodes ordinary page WAL without logical-EOF metadata');
like($dump, qr/rmgr: .*\b/, 'workload emits ordinary page WAL records');

$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres',
	q[SELECT count(*) FROM umbra_physical_eof WHERE payload = repeat('g', 700)]),
	'699', 'redo reads every page created after the checkpoint');
$node->safe_psql('postgres', 'SET enable_seqscan = off;');
is($node->safe_psql('postgres',
	q[SELECT count(*) FROM umbra_physical_eof WHERE payload = repeat('g', 700)]),
	'699', 'bulk-built index remains readable after redo');
$node->safe_psql('postgres', 'SET enable_indexscan = off;');
is($node->safe_psql('postgres', q[
SELECT count(*) FROM umbra_physical_eof
WHERE to_tsvector('simple', payload) @@
      to_tsquery('simple', repeat('g', 700))]),
	'699', 'range-FPI index remains readable after redo');
is(check_physical_eof($node, 'umbra_physical_eof', $main_file, $block_size,
		'redo'), $grown_eof,
	'redo restores the logical EOF through physical length alone');

$node->stop;
done_testing();
