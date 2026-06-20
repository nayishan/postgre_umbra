# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('umbra_skip_wal_dense_map');

$node->init;
$node->append_conf(
	'postgresql.conf', qq[
wal_level = 'minimal'
autovacuum = off
shared_buffers = '256MB'
max_wal_size = '4GB'
min_wal_size = '1GB'
checkpoint_timeout = '1h'
]);
$node->start();

my $start_lsn =
  $node->safe_psql('postgres', q[SELECT pg_current_wal_lsn();]);

$node->safe_psql('postgres', q[
CREATE TABLE umbra_skipwal_dense AS
SELECT g::bigint AS id, repeat('x', 200) AS pad
FROM generate_series(1, 50000) AS g;
]);

my $count =
  $node->safe_psql('postgres', q[SELECT count(*) FROM umbra_skipwal_dense;]);
is($count, '50000', 'skip-WAL-created relation is readable before restart');

my $end_lsn =
  $node->safe_psql('postgres', q[SELECT pg_current_wal_lsn();]);

my ($dump_stdout, $dump_stderr) = run_command(
	[
		'pg_waldump', '-p', $node->data_dir . '/pg_wal',
		'--start',   $start_lsn,
		'--end',     $end_lsn
	]);
is($dump_stderr, '', 'pg_waldump raw dump completed without stderr');

my @dense_lines =
  grep { /desc: SKIP_WAL_CHUNK_BASE/ }
  split /\n/, $dump_stdout;
ok(@dense_lines > 0,
   'raw WAL dump contains skip-WAL chunk-base MAP records');

my @main_dense_lines =
  grep { /fork 0 nblocks ([1-9][0-9]*)/ }
  @dense_lines;
ok(@main_dense_lines > 0,
   'skip-WAL chunk-base MAP record carries concrete MAIN fork nblocks');

$node->stop();

done_testing();
