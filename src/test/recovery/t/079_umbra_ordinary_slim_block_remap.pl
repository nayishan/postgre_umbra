# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

sub update_round
{
	my ($node, $payload) = @_;

	$node->safe_psql('postgres',
		q[SELECT count(*) FROM ordinary_slim_probe WHERE id <= 100000;]);
	$node->safe_psql('postgres', q[CHECKPOINT;]);

	my $start_lsn =
	  $node->safe_psql('postgres', q[SELECT pg_current_wal_lsn();]);

	$node->safe_psql(
		'postgres',
		"UPDATE ordinary_slim_probe
SET payload = repeat('$payload', 80)
WHERE id <= 100000;"
	);

	my $end_lsn =
	  $node->safe_psql('postgres', q[SELECT pg_current_wal_lsn();]);

	$node->safe_psql('postgres', q[
SELECT pg_switch_wal();
CHECKPOINT;
]);

	return ($start_lsn, $end_lsn);
}

sub dump_wal_between
{
	my ($node, $start_lsn, $end_lsn, $desc) = @_;

	my ($dump_stdout, $dump_stderr) = run_command(
		[
			'pg_waldump', '-b', '-p', $node->archive_dir,
			'--start', $start_lsn,
			'--end',   $end_lsn
		]);
	is($dump_stderr, '', "$desc pg_waldump completed without stderr");

	return $dump_stdout;
}

sub shift_header_lines
{
	my ($dump_stdout, $locator) = @_;

	return grep {
		/blkref #\d+: rel \Q$locator\E fork main blk \d+.*; shift: source_slot [0-2] target_slot [0-2] logical_nblocks \d+/
	} split /\n/, $dump_stdout;
}

sub has_slot_transition
{
	my ($shift_lines, $source_slot, $target_slot) = @_;

	return scalar grep {
		/; shift: source_slot $source_slot target_slot $target_slot logical_nblocks \d+/
	} @$shift_lines;
}

my $node = PostgreSQL::Test::Cluster->new('umbra_ordinary_slim_block_remap');

$node->init(has_archiving => 1);
$node->append_conf(
	'postgresql.conf', qq[
wal_level = 'replica'
autovacuum = off
shared_buffers = '256MB'
max_wal_size = '4GB'
min_wal_size = '1GB'
checkpoint_timeout = '1h'
]);
$node->start();

$node->safe_psql('postgres', q[
CREATE TABLE ordinary_slim_probe (id bigint, payload text) WITH (fillfactor = 20);
INSERT INTO ordinary_slim_probe
SELECT g, repeat('x', 80)
FROM generate_series(1, 200000) AS g;
SELECT pg_switch_wal();
]);

my $locator = $node->safe_psql('postgres', q[
SELECT (CASE WHEN c.reltablespace = 0
             THEN d.dattablespace
             ELSE c.reltablespace
        END)::text || '/' ||
       d.oid::text || '/' ||
       pg_relation_filenode(c.oid)::text
FROM pg_class c
JOIN pg_database d ON d.datname = current_database()
WHERE c.oid = 'ordinary_slim_probe'::regclass;
]);

my ($start_lsn_1, $end_lsn_1) = update_round($node, 'y');
my ($start_lsn_2, $end_lsn_2) = update_round($node, 'z');
my ($start_lsn_3, $end_lsn_3) = update_round($node, 'w');
$node->stop();

my $dump_stdout_1 =
  dump_wal_between($node, $start_lsn_1, $end_lsn_1, 'first update');
my $dump_stdout_2 =
  dump_wal_between($node, $start_lsn_2, $end_lsn_2, 'second update');
my $dump_stdout_3 =
  dump_wal_between($node, $start_lsn_3, $end_lsn_3, 'third update');

my @shift_header_lines_1 = shift_header_lines($dump_stdout_1, $locator);
my @shift_header_lines_2 = shift_header_lines($dump_stdout_2, $locator);
my @shift_header_lines_3 = shift_header_lines($dump_stdout_3, $locator);
my @shift_header_lines_all =
  (@shift_header_lines_1, @shift_header_lines_2, @shift_header_lines_3);

ok(@shift_header_lines_1 > 0,
	'first update WAL contains chunk-paired shift headers for updated heap pages');
ok(has_slot_transition(\@shift_header_lines_all, 0, 1),
	'workload rotates heap pages from slot 0 to slot 1');
ok(has_slot_transition(\@shift_header_lines_all, 1, 2),
	'workload rotates heap pages from slot 1 to slot 2');
ok(has_slot_transition(\@shift_header_lines_all, 2, 0),
	'workload rotates heap pages from slot 2 back to slot 0');

done_testing();
