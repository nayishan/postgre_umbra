# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

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
CREATE TABLE ordinary_slim_probe (id bigint, payload text) WITH (fillfactor = 70);
INSERT INTO ordinary_slim_probe
SELECT g, repeat('x', 80)
FROM generate_series(1, 200000) AS g;
CHECKPOINT;
SELECT pg_switch_wal();
]);

my $start_lsn =
  $node->safe_psql('postgres', q[SELECT pg_current_wal_lsn();]);

$node->safe_psql('postgres', q[
UPDATE ordinary_slim_probe
SET payload = repeat('y', 80)
WHERE id <= 100000;
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

my @remap_header_lines =
  grep { /; remap: old_pblk \d+ new_pblk \d+ logical_nblocks \d+ next_free_pblk \d+/ }
  split /\n/, $dump_stdout;

ok(@remap_header_lines > 0,
   'raw WAL dump contains full remap block headers for updated heap pages');

done_testing();
