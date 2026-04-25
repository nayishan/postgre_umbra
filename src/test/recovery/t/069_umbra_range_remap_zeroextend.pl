# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'requires --with-umbra MAP fork'
	unless check_pg_config('^#define USE_UMBRA 1$');

my $node = PostgreSQL::Test::Cluster->new('umbra_range_remap_zeroextend');
my $input = $node->basedir . '/copy_input.csv';

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

open(my $fh, '>', $input) or die "could not create $input: $!";
my $pad = 'x' x 200;
for my $i (1 .. 200_000)
{
	print {$fh} "$i,$pad\n";
}
close($fh);

$node->safe_psql('postgres', q[
CREATE TABLE umbra_range_probe (id bigint, pad text);
SELECT pg_switch_wal();
]);

my $start_lsn =
  $node->safe_psql('postgres', q[SELECT pg_current_wal_lsn();]);

$node->safe_psql('postgres',
	qq[COPY umbra_range_probe FROM '$input' WITH (FORMAT csv);]);

my $end_lsn =
  $node->safe_psql('postgres', q[SELECT pg_current_wal_lsn();]);

$node->safe_psql('postgres', q[
SELECT pg_switch_wal();
CHECKPOINT;
]);
$node->stop();

my ($dump_stdout, $dump_stderr) = run_command(
	[
		'pg_waldump', '-p', $node->archive_dir,
		'--start',   $start_lsn,
		'--end',     $end_lsn
	]);
is($dump_stderr, '', 'pg_waldump raw dump completed without stderr');

my ($stats_stdout, $stats_stderr) = run_command(
	[
		'pg_waldump', '-p', $node->archive_dir,
		'--stats=record',
		'--start', $start_lsn,
		'--end',   $end_lsn
	]);
is($stats_stderr, '', 'pg_waldump stats completed without stderr');
ok($stats_stdout =~ /Umbra\/RANGE_REMAP(?:_COMPACT)?\s+\d+/,
   'WAL stats report Umbra range remap records');

my @main_range_lines =
  grep { /desc: RANGE_REMAP(?:_COMPACT)?/ && $_ !~ /_(?:fsm|vm)\b/ }
  split /\n/, $dump_stdout;
ok(@main_range_lines > 0,
   'raw WAL dump contains main-fork RANGE_REMAP records');

my $main_range_records = 0;
my $main_range_pages = 0;
my $max_main_range = 0;
for my $line (@main_range_lines)
{
	if ($line =~ /count (\d+)/)
	{
		my $count = $1;

		$main_range_records++;
		$main_range_pages += $count;
		$max_main_range = $count if $count > $max_main_range;
	}
}

cmp_ok($max_main_range, '>', 1,
	   'main-fork RANGE_REMAP batches more than one page');
cmp_ok($main_range_pages - $main_range_records, '>', 0,
	   'main-fork RANGE_REMAP collapses multiple first-born pages into fewer WAL records');

done_testing();
