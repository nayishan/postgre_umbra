# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('umbra_hash_birth_block_remap');

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
CREATE TABLE hash_birth_probe (id bigint);
CREATE INDEX hash_birth_probe_idx ON hash_birth_probe USING hash (id);
SELECT pg_switch_wal();
]);

my $start_lsn =
  $node->safe_psql('postgres', q[SELECT pg_current_wal_lsn();]);

$node->safe_psql('postgres', q[
INSERT INTO hash_birth_probe
SELECT g
FROM generate_series(1, 600000) AS g;
]);

my $end_lsn =
  $node->safe_psql('postgres', q[SELECT pg_current_wal_lsn();]);

$node->safe_psql('postgres', q[
SELECT pg_switch_wal();
CHECKPOINT;
]);
$node->stop();

my ($dump_stdout, $dump_stderr) = run_command(
	[
		'pg_waldump', '-b', '-p', $node->archive_dir,
		'--start', $start_lsn,
		'--end',   $end_lsn
	]);
is($dump_stderr, '', 'pg_waldump block dump completed without stderr');

my @shift_header_lines =
  grep { /; shift: shifted_to_shadow (?:true|false) logical_nblocks \d+/ }
  split /\n/, $dump_stdout;

ok(@shift_header_lines > 0,
   'raw WAL dump contains chunk-paired shift headers for hash index pages');

done_testing();
